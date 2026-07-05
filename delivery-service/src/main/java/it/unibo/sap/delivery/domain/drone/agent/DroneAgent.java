package it.unibo.sap.delivery.domain.drone.agent;

import it.unibo.sap.delivery.domain.drone.env.DroneEnvironment;
import it.unibo.sap.delivery.domain.drone.env.EnvTimeElapsed;
import it.unibo.sap.delivery.domain.fleet.Coordinates;
import it.unibo.sap.delivery.domain.fleet.Drone;
import it.unibo.sap.delivery.domain.fleet.DroneTelemetrySink;
import it.unibo.sap.delivery.domain.fleet.Position;

public class DroneAgent extends BasicAgentArch {

    private enum State { READY_TO_SHIP, SHIPPING, DELIVERED }

    private static final double STEP_FRACTION = 0.05;

    private final DroneEnvironment environment;
    private final Drone drone;
    private final String deliveryId;
    private final Coordinates destination;
    private final DroneTelemetrySink sink;
    private final double arrivalThreshold;

    private State state;
    private long currentTime;

    public DroneAgent(final DroneEnvironment environment,
                      final Drone drone,
                      final String deliveryId,
                      final Coordinates destination,
                      final DroneTelemetrySink sink,
                      final double arrivalThreshold) {
        super("drone-agent-" + deliveryId);
        this.environment = environment;
        this.drone = drone;
        this.deliveryId = deliveryId;
        this.destination = destination;
        this.sink = sink;
        this.arrivalThreshold = arrivalThreshold;
    }

    public void startDrone() {
        start();
    }

    @Override
    protected void init() {
        this.state = State.READY_TO_SHIP;
    }

    @Override
    protected void sense(final Percept percept) {
        if (percept.event() instanceof EnvTimeElapsed tick) {
            this.currentTime = tick.currentTimeMillis();
        }
    }

    @Override
    protected void plan() {
        if (state == State.DELIVERED) {
            return;
        }
        final Coordinates current = drone.getPosition().coordinates();
        final double distance = current.euclideanDistanceTo(destination);
        if (distance < arrivalThreshold) {
            state = State.DELIVERED;
            scheduleAction(this::delivered);
        } else if (state == State.READY_TO_SHIP) {
            state = State.SHIPPING;
            scheduleAction(this::shipDelivery);
        } else {
            scheduleAction(this::timeElapsed);
        }
    }

    private void shipDelivery() {
        stepTowardDestination();
    }

    private void timeElapsed() {
        stepTowardDestination();
    }

    private void stepTowardDestination() {
        final Coordinates current = drone.getPosition().coordinates();
        final Coordinates next = step(current, destination);
        drone.updatePosition(Position.at(next.latitude(), next.longitude()));
        sink.onPositionUpdated(deliveryId, next.latitude(), next.longitude());
    }

    private void delivered() {
        drone.updatePosition(Position.at(destination.latitude(), destination.longitude()));
        drone.arrived();
        sink.onArrived(deliveryId, destination.latitude(), destination.longitude());
        stop();
        environment.unregister(this);
    }

    private static Coordinates step(final Coordinates from, final Coordinates to) {
        final double lat = from.latitude() + (to.latitude() - from.latitude()) * STEP_FRACTION;
        final double lon = from.longitude() + (to.longitude() - from.longitude()) * STEP_FRACTION;
        return new Coordinates(lat, lon);
    }
}
