package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import it.unibo.sap.common.hexagonal.OutputAdapter;
import it.unibo.sap.delivery.application.TrackingSessionEventObserver;
import it.unibo.sap.delivery.domain.deliveries.EstimatedTimeRemaining;
import it.unibo.sap.delivery.kafka.OutputEventChannel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaTrackingSessionEventObserver implements TrackingSessionEventObserver, OutputAdapter {

    private final Vertx vertx;
    private final String address;
    private final Map<String, OutputEventChannel> internalChannels = new ConcurrentHashMap<>();

    public KafkaTrackingSessionEventObserver(final Vertx vertx, final String address) {
        this.vertx = vertx;
        this.address = address;
    }

    @Override
    public void pushTrackingUpdate(final String deliveryId,
                                   final String status,
                                   final double latitude,
                                   final double longitude,
                                   final long estimatedTimeRemainingSeconds) {
        final JsonObject update = new JsonObject()
                .put("event", "TRACKING_UPDATE")
                .put("deliveryId", deliveryId)
                .put("status", status)
                .put("position", new JsonObject()
                        .put("latitude", latitude)
                        .put("longitude", longitude))
                .put("estimatedTimeRemainingSeconds", estimatedTimeRemainingSeconds)
                .put("estimatedTimeRemainingFormatted",
                        EstimatedTimeRemaining.formatSeconds(estimatedTimeRemainingSeconds));
        internalChannel(deliveryId).postEvent(update);
    }

    private OutputEventChannel internalChannel(final String deliveryId) {
        return internalChannels.computeIfAbsent(deliveryId,
                id -> new OutputEventChannel(vertx, "delivery-tracking-" + id + "-internal-events", address));
    }
}
