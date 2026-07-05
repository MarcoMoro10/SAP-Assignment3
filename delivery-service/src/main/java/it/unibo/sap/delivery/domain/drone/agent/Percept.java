package it.unibo.sap.delivery.domain.drone.agent;

import it.unibo.sap.delivery.domain.drone.env.EnvironmentEvent;

public record Percept(EnvironmentEvent event, long time) {
}
