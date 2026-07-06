package it.unibo.sap.gateway.support;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;

/**
 * Finto delivery-service lato Kafka per gli integration test del gateway: consuma i canali di comando
 * (create/get/cancel/track) e risponde sui canali approved/rejected secondo il contratto reale del
 * DeliveryServiceController, cosi' il {@code DeliveryServiceProxy} puo' essere testato ATTRAVERSO il
 * suo contratto Kafka (request/reply + mappatura errorType -> statusCode) senza importare il modulo
 * delivery-service (opzione B, moduli autonomi).
 */
public final class FakeDeliveryKafka {

    private static final String TRACKING_SESSION_SUFFIX = "-TRK";

    private final KafkaProducer<String, String> producer;

    public FakeDeliveryKafka(final Vertx vertx) {
        this.producer = KafkaTestSupport.producer(vertx);
        KafkaTestSupport.consumer(vertx, "create-delivery-requests", this::onCreate);
        KafkaTestSupport.consumer(vertx, "get-delivery-detail-requests", this::onGet);
        KafkaTestSupport.consumer(vertx, "cancel-delivery-requests", this::onCancel);
        KafkaTestSupport.consumer(vertx, "delivery-tracking-requests", this::onTrack);
    }

    public static String trackingSessionFor(final String deliveryId) {
        return deliveryId + TRACKING_SESSION_SUFFIX;
    }

    private void onCreate(final JsonObject req) {
        final String requestId = req.getString("requestId");
        if (req.getDouble("weight", 0.0) > 10.0) {
            post("create-delivery-requests-rejected", new JsonObject()
                    .put("requestId", requestId)
                    .put("reason", "No drone can carry this package")
                    .put("errorType", "VALIDATION_REJECTED"));
        } else {
            post("create-delivery-requests-approved", new JsonObject()
                    .put("requestId", requestId)
                    .put("deliveryId", "DLV-1")
                    .put("status", "IN_PROGRESS")
                    .put("assignedDroneId", "DRN-1")
                    .put("echoWeight", req.getValue("weight")));
        }
    }

    private void onGet(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        if ("user-1".equals(req.getString("senderId"))) {
            post("get-delivery-" + deliveryId + "-detail-requests-approved", new JsonObject()
                    .put("requestId", requestId).put("deliveryId", deliveryId).put("status", "IN_PROGRESS"));
        } else {
            post("get-delivery-" + deliveryId + "-detail-requests-rejected", new JsonObject()
                    .put("requestId", requestId).put("deliveryId", deliveryId)
                    .put("reason", "Delivery not found").put("errorType", "NOT_FOUND"));
        }
    }

    private void onCancel(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        post("cancel-delivery-" + deliveryId + "-requests-approved", new JsonObject()
                .put("requestId", requestId).put("deliveryId", deliveryId).put("status", "CANCELLED"));
    }

    private void onTrack(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        post("delivery-" + deliveryId + "-tracking-requests-approved", new JsonObject()
                .put("requestId", requestId).put("deliveryId", deliveryId)
                .put("trackingSessionId", trackingSessionFor(deliveryId)));
    }

    private void post(final String topic, final JsonObject payload) {
        KafkaTestSupport.post(producer, topic, payload);
    }
}
