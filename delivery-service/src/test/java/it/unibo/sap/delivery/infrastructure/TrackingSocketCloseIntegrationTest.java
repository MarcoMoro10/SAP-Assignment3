package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import it.unibo.sap.delivery.support.KafkaTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifica che il {@link KafkaTrackingSessionEventObserver} pubblichi il frame terminale
 * ({@code DELIVERED}/ETR-0) sul topic interno {@code delivery-tracking-{deliveryId}-internal-events}.
 * La chiusura del WebSocket lato client sul frame terminale e' ora responsabilita' del bridge del
 * gateway (coperta da TrackingRelayIntegrationTest lato api-gateway): qui verifichiamo il contributo
 * del delivery-service, cioe' la produzione dell'evento terminale sul canale Kafka. Richiede un broker.
 */
class TrackingSocketCloseIntegrationTest {

    private static final String DELIVERY_ID = "DLV-1";

    private static Vertx vertx;

    @BeforeAll
    static void start() {
        KafkaTestSupport.assumeBrokerReachable();
        vertx = Vertx.vertx();
    }

    @AfterAll
    static void stop() {
        if (vertx != null) {
            final CompletableFuture<Void> closed = new CompletableFuture<>();
            vertx.close().onComplete(ar -> closed.complete(null));
            try {
                closed.get(15, TimeUnit.SECONDS);
            } catch (final Exception ignored) {
                // best-effort teardown
            }
        }
    }

    @Test
    void observerPublishesTheTerminalDeliveredFrameOnTheInternalTopic() throws Exception {
        final KafkaTrackingSessionEventObserver observer =
                new KafkaTrackingSessionEventObserver(vertx, KafkaTestSupport.brokerAddress());

        final CompletableFuture<JsonObject> terminal = new CompletableFuture<>();
        KafkaTestSupport.consumer(vertx, "delivery-tracking-" + DELIVERY_ID + "-internal-events", frame -> {
            if ("DELIVERED".equals(frame.getString("status")) && !terminal.isDone()) {
                terminal.complete(frame);
            }
        });

        vertx.setTimer(1500, t -> observer.pushTrackingUpdate(DELIVERY_ID, "DELIVERED", 44.50, 11.35, 0L));

        final JsonObject frame = terminal.get(20, TimeUnit.SECONDS);
        assertEquals("TRACKING_UPDATE", frame.getString("event"));
        assertEquals("DELIVERED", frame.getString("status"));
        assertEquals(0L, frame.getLong("estimatedTimeRemainingSeconds"));
    }
}
