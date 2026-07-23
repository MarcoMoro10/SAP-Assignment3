package it.unibo.sap.gateway.application;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import it.unibo.sap.common.hexagonal.OutputPort;

import java.util.Optional;

public interface DeliveryService extends OutputPort {

    Future<Boolean> pingHealth();

    JsonObject createDelivery(JsonObject request, String sessionId);

    JsonObject cancelDelivery(String deliveryId, String sessionId);

    Optional<JsonObject> getDelivery(String deliveryId, String sessionId);

    JsonObject trackDelivery(String deliveryId, String sessionId);

    JsonObject viewFleet(String sessionId);

    JsonObject viewScheduling(String droneId, String sessionId);
}
