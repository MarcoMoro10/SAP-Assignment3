package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import it.unibo.sap.delivery.application.DeliveryService;
import it.unibo.sap.delivery.application.DeliveryServiceImpl;
import it.unibo.sap.delivery.infrastructure.fleet.FleetModule;
import it.unibo.sap.delivery.infrastructure.fleet.FleetSeeder;
import it.unibo.sap.delivery.infrastructure.fleet.InMemoryDroneRepository;
import it.unibo.sap.delivery.kafka.OutputEventChannel;
import it.unibo.sap.delivery.support.FakeGeocodingPort;
import it.unibo.sap.delivery.support.FakeSessionValidator;
import it.unibo.sap.delivery.support.InMemoryDeliveryRepository;
import it.unibo.sap.delivery.support.KafkaTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DeliveryCommandAuthorizationIntegrationTest {

    private static final int PORT = 9099;
    private static final long TIMEOUT_S = 20;

    private static Vertx vertx;
    private static FleetModule fleetModule;
    private static OutputEventChannel createIn;
    private static KafkaConsumer<String, String> approvedConsumer;
    private static KafkaConsumer<String, String> rejectedConsumer;

    private static final Map<String, CompletableFuture<JsonObject>> pending = new ConcurrentHashMap<>();

    @BeforeAll
    static void startService() throws Exception {
        KafkaTestSupport.assumeBrokerReachable();
        vertx = Vertx.vertx();
        final InMemoryDroneRepository drones = new InMemoryDroneRepository();
        FleetSeeder.seed(drones);
        fleetModule = new FleetModule(drones, 0.01);
        final DeliveryService deliveryService = new DeliveryServiceImpl(
                new InMemoryDeliveryRepository(), fleetModule, new FakeGeocodingPort(),
                new InMemoryTrackingSessionRegistry());
        final RequestAuthorizer authorizer = new RequestAuthorizer(new FakeSessionValidator());

        final CompletableFuture<Void> deployed = new CompletableFuture<>();
        vertx.deployVerticle(new DeliveryServiceController(
                        deliveryService, authorizer, PORT, KafkaTestSupport.brokerAddress()))
                .onComplete(ar -> deployed.complete(null));
        deployed.get(20, TimeUnit.SECONDS);

        approvedConsumer = KafkaTestSupport.consumer(vertx, "create-delivery-requests-approved",
                ev -> complete(ev, false));
        rejectedConsumer = KafkaTestSupport.consumer(vertx, "create-delivery-requests-rejected",
                ev -> complete(ev, true));
        createIn = new OutputEventChannel(vertx, "create-delivery-requests", KafkaTestSupport.brokerAddress());
        Thread.sleep(2000);
    }

    private static void complete(final JsonObject ev, final boolean rejected) {
        final CompletableFuture<JsonObject> f = pending.get(ev.getString("requestId"));
        if (f != null) {
            f.complete(ev.put("__rejected", rejected));
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (approvedConsumer != null) {
            approvedConsumer.unsubscribe();
        }
        if (rejectedConsumer != null) {
            rejectedConsumer.unsubscribe();
        }
        if (fleetModule != null) {
            fleetModule.stop();
        }
        if (vertx != null) {
            final CompletableFuture<Void> closed = new CompletableFuture<>();
            vertx.close().onComplete(ar -> closed.complete(null));
            closed.get(15, TimeUnit.SECONDS);
        }
    }

    private JsonObject postCreate(final Map<String, Object> identity) throws Exception {
        final String requestId = UUID.randomUUID().toString();
        final CompletableFuture<JsonObject> reply = new CompletableFuture<>();
        pending.put(requestId, reply);
        final JsonObject payload = new JsonObject()
                .put("requestId", requestId)
                .put("weight", 2.0)
                .put("startingPlace", new JsonObject().put("street", "via Emilia").put("number", 9))
                .put("destinationPlace", new JsonObject().put("street", "via Veneto").put("number", 5))
                .put("immediate", true)
                .put("deadlineMinutes", 60);
        identity.forEach(payload::put);
        createIn.postEvent(payload);
        return reply.get(TIMEOUT_S, TimeUnit.SECONDS);
    }

    @Test
    void validSessionAuthorizesAndCreates() throws Exception {
        final JsonObject reply = postCreate(Map.of("sessionId", "user-1"));   // SENDER
        assertEquals(Boolean.FALSE, reply.getBoolean("__rejected"), "a valid SENDER session is approved");
        assertEquals("IN_PROGRESS", reply.getString("status"));
    }

    @Test
    void invalidSessionIsRejectedAsUnauthorizedNotInternal() throws Exception {
        final JsonObject reply = postCreate(Map.of("sessionId", FakeSessionValidator.INVALID_TOKEN));
        assertEquals(Boolean.TRUE, reply.getBoolean("__rejected"));
        assertEquals("UNAUTHORIZED", reply.getString("errorType"), "explicit 401, not 500");
        assertEquals("Invalid or expired session", reply.getString("reason"));
        assertNull(reply.getString("deliveryId"), "no delivery is created for an unauthorized command");
    }

    @Test
    void wrongRoleIsRejectedAsForbidden() throws Exception {
        final JsonObject reply = postCreate(Map.of("sessionId", "admin-1"));   // ADMIN on a SENDER command
        assertEquals(Boolean.TRUE, reply.getBoolean("__rejected"));
        assertEquals("FORBIDDEN", reply.getString("errorType"), "explicit 403, not 500");
        assertEquals("Forbidden: requires SENDER role", reply.getString("reason"));
    }

    @Test
    void legacySenderIdPathIsUnchanged() throws Exception {
        final JsonObject reply = postCreate(Map.of("senderId", "user-1"));     // no sessionId => legacy
        assertEquals(Boolean.FALSE, reply.getBoolean("__rejected"), "the legacy senderId path still works");
        assertEquals("IN_PROGRESS", reply.getString("status"));
    }
}
