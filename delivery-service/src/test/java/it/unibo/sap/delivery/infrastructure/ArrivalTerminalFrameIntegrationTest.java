package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import it.unibo.sap.delivery.application.CreateDeliveryCommand;
import it.unibo.sap.delivery.application.CreateDeliveryResult;
import it.unibo.sap.delivery.application.DeliveryService;
import it.unibo.sap.delivery.application.DeliveryServiceImpl;
import it.unibo.sap.delivery.application.DroneEventHandler;
import it.unibo.sap.delivery.application.TrackingSessionRegistry;
import it.unibo.sap.delivery.support.FakeFleetPort;
import it.unibo.sap.delivery.support.FakeGeocodingPort;
import it.unibo.sap.delivery.support.InMemoryDeliveryRepository;
import it.unibo.sap.delivery.support.KafkaTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test (Kafka) dell'arrivo: guida la vera catena
 * {@link DroneEventHandler#onDroneArrived} (non un evento pubblicato a mano) con un
 * {@link KafkaTrackingSessionEventObserver}, e verifica che il frame terminale {@code DELIVERED}/ETR-0
 * finisca sul topic Kafka {@code delivery-tracking-{deliveryId}-internal-events}. Usa un
 * {@link FakeFleetPort} cosi' nessun agente di background muove il drone. Richiede un broker reale.
 */
class ArrivalTerminalFrameIntegrationTest {

    private static final String SENDER_ID = "user-1";
    private static final double DRONE_SPEED_UNITS_PER_SECOND = 0.01;

    private static Vertx vertx;
    private static DeliveryService deliveryService;
    private static DroneEventHandler droneEventHandler;

    @BeforeAll
    static void startService() {
        KafkaTestSupport.assumeBrokerReachable();
        vertx = Vertx.vertx();
        final InMemoryDeliveryRepository repository = new InMemoryDeliveryRepository();
        final TrackingSessionRegistry trackingRegistry = new InMemoryTrackingSessionRegistry();
        final FakeGeocodingPort geocoding = new FakeGeocodingPort();
        final FakeFleetPort fleet = new FakeFleetPort();
        final KafkaTrackingSessionEventObserver observer =
                new KafkaTrackingSessionEventObserver(vertx, KafkaTestSupport.brokerAddress());

        deliveryService = new DeliveryServiceImpl(repository, fleet, geocoding, trackingRegistry);
        droneEventHandler = new DroneEventHandler(
                repository, trackingRegistry, observer, fleet, DRONE_SPEED_UNITS_PER_SECOND);
    }

    @AfterAll
    static void stopService() {
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
    void onDroneArrivedPublishesTheFinalDeliveredFrameOnTheInternalTopic() throws Exception {
        final CreateDeliveryResult created = deliveryService.createDelivery(new CreateDeliveryCommand(
                SENDER_ID, 2.0, "via Emilia", 9, "via Veneto", 5, true, null, 60));
        final String deliveryId = created.deliveryId();
        deliveryService.startTracking(deliveryId, SENDER_ID);

        final CompletableFuture<JsonObject> delivered = new CompletableFuture<>();
        KafkaTestSupport.consumer(vertx, "delivery-tracking-" + deliveryId + "-internal-events", frame -> {
            if ("DELIVERED".equals(frame.getString("status")) && !delivered.isDone()) {
                delivered.complete(frame);
            }
        });

        vertx.setTimer(1500, t -> {
            droneEventHandler.onDronePositionUpdated(deliveryId, 44.49, 11.34);
            droneEventHandler.onDroneArrived(deliveryId, 44.50, 11.34);
        });

        final JsonObject frame = delivered.get(20, TimeUnit.SECONDS);
        assertEquals("DELIVERED", frame.getString("status"));
        assertEquals(0L, frame.getLong("estimatedTimeRemainingSeconds"));
    }
}
