package it.unibo.sap.delivery.domain.drone.env;

public interface EnvironmentObserver {

    void onEnvironmentEvent(EnvironmentEvent event);
}
