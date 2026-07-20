package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.core.buffer.Buffer;
import it.unibo.sap.delivery.application.DeliveryService;
import it.unibo.sap.delivery.application.DeliveryServiceImpl;
import it.unibo.sap.delivery.infrastructure.fleet.FleetModule;
import it.unibo.sap.delivery.infrastructure.fleet.InMemoryDroneRepository;
import it.unibo.sap.delivery.support.FakeGeocodingPort;
import it.unibo.sap.delivery.support.FakeSessionValidator;
import it.unibo.sap.delivery.support.InMemoryDeliveryRepository;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FleetMonitoringAuthorizationTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9098;

    private static Vertx vertx;
    private static WebClient webClient;
    private static FleetModule fleetModule;

    @BeforeAll
    static void startService() throws Exception {
        vertx = Vertx.vertx();
        fleetModule = new FleetModule(new InMemoryDroneRepository(), 0.01);
        final DeliveryService deliveryService = new DeliveryServiceImpl(
                new InMemoryDeliveryRepository(), fleetModule, new FakeGeocodingPort(),
                new InMemoryTrackingSessionRegistry());
        final RequestAuthorizer authorizer = new RequestAuthorizer(new FakeSessionValidator());

        final CompletableFuture<Void> deployed = new CompletableFuture<>();
        vertx.deployVerticle(new FleetMonitoringController(deliveryService, authorizer, PORT))
                .onComplete(ar -> deployed.complete(null));
        deployed.get(15, TimeUnit.SECONDS);
        webClient = WebClient.create(vertx);
    }

    @AfterAll
    static void stopService() throws Exception {
        if (fleetModule != null) {
            fleetModule.stop();
        }
        if (vertx != null) {
            final CompletableFuture<Void> closed = new CompletableFuture<>();
            vertx.close().onComplete(ar -> closed.complete(null));
            closed.get(15, TimeUnit.SECONDS);
        }
    }

    private HttpResponse<Buffer> getFleet(final String sessionId) throws Exception {
        final CompletableFuture<HttpResponse<Buffer>> future = new CompletableFuture<>();
        var request = webClient.get(PORT, HOST, "/api/v1/admin/fleet");
        if (sessionId != null) {
            request = request.putHeader("X-Session-Id", sessionId);
        }
        request.send(ar -> {
            if (ar.succeeded()) {
                future.complete(ar.result());
            } else {
                future.completeExceptionally(ar.cause());
            }
        });
        return future.get(15, TimeUnit.SECONDS);
    }

    @Test
    void aSenderIsForbiddenFromTheAdminFleetView() throws Exception {
        final HttpResponse<Buffer> response = getFleet("user-1");   // FakeSessionValidator => SENDER
        assertEquals(403, response.statusCode());
        assertEquals("Forbidden: requires ADMIN role",
                response.bodyAsJsonObject().getString("error"));
    }

    @Test
    void anAdminMaySeeTheFleetView() throws Exception {
        assertEquals(200, getFleet("admin-1").statusCode());
    }

    @Test
    void withoutAnIdentityTheAdminViewIsRejected() throws Exception {
        assertEquals(401, getFleet(null).statusCode());
    }
}
