package it.unibo.sap.gateway.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import it.unibo.sap.gateway.application.DeliveryService;
import it.unibo.sap.gateway.support.FakeDeliveryKafka;
import it.unibo.sap.gateway.support.KafkaTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test (Kafka) del {@link DeliveryServiceProxy} contro un finto delivery-service lato Kafka
 * ({@link FakeDeliveryKafka}): verifica che il proxy traduca create/get/cancel in request/reply sui
 * canali Kafka e mappi le risposte approved/rejected (errorType -> statusCode). L'admin (viewFleet)
 * resta HTTP verso uno stub. Richiede un broker reale (skippato senza).
 */
class DeliveryServiceProxyIntegrationTest {

    private static final int ADMIN_STUB_PORT = 9402;
    private static final String HOST = "localhost";

    private static Vertx vertx;
    private static WebClient webClient;
    private static DeliveryService proxy;

    @BeforeAll
    static void startStub() {
        KafkaTestSupport.assumeBrokerReachable();
        vertx = Vertx.vertx();

        new FakeDeliveryKafka(vertx);

        final Router router = Router.router(vertx);
        router.route("/api/v1/*").handler(BodyHandler.create());
        router.get("/api/v1/admin/fleet").handler(ctx ->
                ctx.response().setStatusCode(200).putHeader("Content-Type", "application/json")
                        .end(new JsonArray().add(new JsonObject()
                                .put("droneId", "DRN-1").put("status", "AVAILABLE")).encode()));
        final CountDownLatch latch = new CountDownLatch(1);
        vertx.createHttpServer().requestHandler(router).listen(ADMIN_STUB_PORT).onComplete(ar -> latch.countDown());
        await(latch);

        webClient = WebClient.create(vertx);
        proxy = new DeliveryServiceProxy(
                vertx, webClient, HOST, ADMIN_STUB_PORT, ADMIN_STUB_PORT, KafkaTestSupport.brokerAddress());

       try {
            Thread.sleep(5000);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @AfterAll
    static void stopStub() {
        if (vertx != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            vertx.close().onComplete(ar -> latch.countDown());
            await(latch);
        }
    }

    @Test
    void createDeliveryForwardsBodyAndReturnsStatusAndPayload() {
        final JsonObject result = proxy.createDelivery(new JsonObject().put("weight", 2.0).put("senderId", "user-1"));

        assertEquals(201, result.getInteger("_statusCode"));
        assertEquals("DLV-1", result.getString("deliveryId"));
        assertEquals("DRN-1", result.getString("assignedDroneId"));
        assertEquals(2.0, result.getDouble("echoWeight"), 1e-9);
    }

    @Test
    void createDeliveryMapsAValidationRejectionTo422() {
        final JsonObject result = proxy.createDelivery(new JsonObject().put("weight", 12.0).put("senderId", "user-1"));

        assertEquals(422, result.getInteger("_statusCode"));
        assertEquals("No drone can carry this package", result.getString("error"));
    }

    @Test
    void getDeliveryReturnsThePayloadForTheOwner() {
        final Optional<JsonObject> result = proxy.getDelivery("DLV-1", "user-1");
        assertTrue(result.isPresent());
        assertEquals("IN_PROGRESS", result.get().getString("status"));
    }

    @Test
    void getDeliveryReturnsEmptyWhenDownstreamRejectsNotFound() {
        final Optional<JsonObject> result = proxy.getDelivery("DLV-1", "intruder");
        assertFalse(result.isPresent());
    }

    @Test
    void cancelDeliveryReturnsCancelledStatus() {
        final JsonObject result = proxy.cancelDelivery("DLV-1", "user-1");
        assertEquals("CANCELLED", result.getString("status"));
    }

    @Test
    void viewFleetWrapsTheDownstreamArray() {
        final JsonObject result = proxy.viewFleet();
        final JsonArray fleet = result.getJsonArray("fleet");
        assertEquals(1, fleet.size());
        assertEquals("DRN-1", fleet.getJsonObject(0).getString("droneId"));
    }

    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting for the delivery-service stub");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while setting up the stub", e);
        }
    }
}
