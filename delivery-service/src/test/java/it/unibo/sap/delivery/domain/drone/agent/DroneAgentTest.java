package it.unibo.sap.delivery.domain.drone.agent;

import it.unibo.sap.delivery.domain.drone.env.DroneEnvironment;
import it.unibo.sap.delivery.domain.drone.env.EnvTimeElapsed;
import it.unibo.sap.delivery.domain.fleet.Coordinates;
import it.unibo.sap.delivery.domain.fleet.Drone;
import it.unibo.sap.delivery.domain.fleet.DroneId;
import it.unibo.sap.delivery.domain.fleet.DroneStatus;
import it.unibo.sap.delivery.domain.fleet.DroneTelemetrySink;
import it.unibo.sap.delivery.domain.fleet.PayloadCapacity;
import it.unibo.sap.delivery.domain.fleet.Position;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test dell'agente (dominio puro: nessun Kafka, nessun thread). Guida deterministicamente il
 * ciclo {@code init/sense/plan/act} (metodi protected, stesso package) e verifica la macchina a stati
 * READY_TO_SHIP -> SHIPPING -> DELIVERED e l'avanzamento a coordinate (step 5%, arrivo sotto soglia ->
 * onArrived + stato ARRIVED), catturando la telemetria con un {@link DroneTelemetrySink} fake.
 */
class DroneAgentTest {

    private static final class RecordingSink implements DroneTelemetrySink {
        private final List<double[]> positions = new ArrayList<>();
        private double[] arrivedAt;
        private int arrivedCount;

        @Override
        public void onPositionUpdated(final String deliveryId, final double lat, final double lon) {
            positions.add(new double[]{lat, lon});
        }

        @Override
        public void onArrived(final String deliveryId, final double lat, final double lon) {
            arrivedAt = new double[]{lat, lon};
            arrivedCount++;
        }
    }

    private static Drone droneAt(final String id, final Coordinates at) {
        final Drone drone = Drone.create(DroneId.of(id), Position.at(at), new PayloadCapacity(5.0));
        drone.assign("DLV-" + id);
        drone.startDelivery();
        return drone;
    }

    @Test
    void advancesFivePercentEachTickThenArrivesSnapsAndStops() {
        final Coordinates start = new Coordinates(44.49, 11.34);
        final Coordinates dest = new Coordinates(44.55, 11.40);
        final Drone drone = droneAt("A", start);
        final double threshold = 0.01 * 0.5; // speed * ARRIVAL_THRESHOLD_FACTOR
        final RecordingSink sink = new RecordingSink();
        final DroneAgent agent = new DroneAgent(new DroneEnvironment(), drone, "DLV-A", dest, sink, threshold);

        agent.init();
        double prevDistance = Double.MAX_VALUE;
        int ticks = 0;
        while (sink.arrivedCount == 0 && ticks < 1000) {
            ticks++;
            agent.sense(new Percept(new EnvTimeElapsed(ticks), ticks));
            agent.plan();
            agent.act();
            final double distance = drone.getPosition().coordinates().euclideanDistanceTo(dest);
            assertTrue(distance <= prevDistance + 1e-12, "distance to destination must not increase");
            prevDistance = distance;
        }

        assertEquals(1, sink.arrivedCount, "onArrived must be called exactly once");
        assertEquals(DroneStatus.ARRIVED, drone.getStatus());

        assertTrue(sink.positions.size() >= 2, "must emit multiple position updates while shipping");

        final double[] first = sink.positions.get(0);
        assertEquals(start.latitude() + (dest.latitude() - start.latitude()) * 0.05, first[0], 1e-12);
        assertEquals(start.longitude() + (dest.longitude() - start.longitude()) * 0.05, first[1], 1e-12);

        assertEquals(dest.latitude(), sink.arrivedAt[0], 1e-12);
        assertEquals(dest.longitude(), sink.arrivedAt[1], 1e-12);
        assertTrue(drone.getPosition().coordinates().euclideanDistanceTo(dest) < threshold);
    }

    @Test
    void deliversImmediatelyWithoutStepsWhenAlreadyWithinThreshold() {
        final Coordinates dest = new Coordinates(44.50, 11.35);
        final Drone drone = droneAt("N", new Coordinates(44.5001, 11.3501));
        final RecordingSink sink = new RecordingSink();
        final DroneAgent agent = new DroneAgent(new DroneEnvironment(), drone, "DLV-N", dest, sink, 0.005);

        agent.init();
        agent.sense(new Percept(new EnvTimeElapsed(1), 1));
        agent.plan();
        agent.act();

        assertEquals(1, sink.arrivedCount, "already within threshold -> delivered on the first tick");
        assertTrue(sink.positions.isEmpty(), "no shipping step when already at destination");
        assertEquals(DroneStatus.ARRIVED, drone.getStatus());
    }
}
