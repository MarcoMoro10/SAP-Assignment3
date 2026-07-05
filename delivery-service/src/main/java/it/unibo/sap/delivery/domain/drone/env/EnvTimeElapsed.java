package it.unibo.sap.delivery.domain.drone.env;

public record EnvTimeElapsed(long currentTimeMillis) implements EnvironmentEvent {
}
