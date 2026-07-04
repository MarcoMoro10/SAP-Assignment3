package it.unibo.sap.gateway.infrastructure;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import it.unibo.sap.common.hexagonal.OutputAdapter;
import it.unibo.sap.gateway.application.DeliveryService;
import it.unibo.sap.gateway.kafka.InputEventChannel;
import it.unibo.sap.gateway.kafka.OutputEventChannel;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DeliveryServiceProxy implements DeliveryService, OutputAdapter {

    // Canali statici di input (comandi di dominio)
    private static final String CH_CREATE_REQUESTS = "create-delivery-requests";
    private static final String CH_CREATE_APPROVED = "create-delivery-requests-approved";
    private static final String CH_CREATE_REJECTED = "create-delivery-requests-rejected";
    private static final String CH_GET_DETAIL_REQUESTS = "get-delivery-detail-requests";
    private static final String CH_CANCEL_REQUESTS = "cancel-delivery-requests";
    private static final String CH_TRACK_REQUESTS = "delivery-tracking-requests";
    private static final String CH_STOP_TRACKING_REQUESTS = "stop-tracking-requests";

    private static final long HEALTH_TIMEOUT_MS = 2000;
    private static final long ADMIN_TIMEOUT_MS = 10_000;
    private static final long REQUEST_TIMEOUT_SECONDS = 10;

    private final Vertx vertx;
    private final WebClient webClient;
    private final String host;
    private final int port;       // delivery HTTP (health) 9002
    private final int fleetPort;  // admin HTTP 9003
    private final String address; // broker Kafka

    // Request/reply: requestId -> Promise; sottoscrizioni e producer riusati per canale.
    private final Map<String, Promise<JsonObject>> pending = new ConcurrentHashMap<>();
    private final Map<String, InputEventChannel> subscriptions = new ConcurrentHashMap<>();
    private final Map<String, OutputEventChannel> outChannels = new ConcurrentHashMap<>();
    // Riconciliazione chiave: trackingSessionId (client/EventBus) -> deliveryId (Kafka).
    private final Map<String, String> sessionToDelivery = new ConcurrentHashMap<>();

    public DeliveryServiceProxy(final Vertx vertx, final WebClient webClient, final String host,
                                final int port, final int fleetPort, final String eventChannelsLocation) {
        this.vertx = vertx;
        this.webClient = webClient;
        this.host = host;
        this.port = port;
        this.fleetPort = fleetPort;
        this.address = eventChannelsLocation;
    }

    // ---- Health del delivery (HTTP, invariato, SENZA circuit breaker) ----
    public Future<Boolean> pingHealth() {
        return webClient.get(port, host, "/api/v1/health")
                .timeout(HEALTH_TIMEOUT_MS).send()
                .map(resp -> resp.statusCode() == 200)
                .otherwise(false);
    }

    // ---- Comandi di dominio via Kafka ----
    @Override
    public JsonObject createDelivery(final JsonObject request) {
        final JsonObject reply = requestReply(CH_CREATE_REQUESTS, CH_CREATE_APPROVED, CH_CREATE_REJECTED,
                request.copy());
        return isApproved(reply)
                ? clean(reply).put("_statusCode", 201)
                : rejectedBody(reply, mapCreateStatus(reply.getString("errorType")));
    }

    @Override
    public JsonObject cancelDelivery(final String deliveryId, final String senderId) {
        final JsonObject reply = requestReply(CH_CANCEL_REQUESTS,
                dyn("cancel-delivery-", deliveryId, "-requests-approved"),
                dyn("cancel-delivery-", deliveryId, "-requests-rejected"),
                new JsonObject().put("deliveryId", deliveryId).put("senderId", senderId));
        return isApproved(reply)
                ? clean(reply).put("_statusCode", 200)
                : rejectedBody(reply, mapCancelStatus(reply.getString("errorType")));
    }

    @Override
    public Optional<JsonObject> getDelivery(final String deliveryId, final String senderId) {
        try {
            final JsonObject reply = requestReply(CH_GET_DETAIL_REQUESTS,
                    dyn("get-delivery-", deliveryId, "-detail-requests-approved"),
                    dyn("get-delivery-", deliveryId, "-detail-requests-rejected"),
                    new JsonObject().put("deliveryId", deliveryId).put("senderId", senderId));
            return isApproved(reply) ? Optional.of(clean(reply)) : Optional.empty();
        } catch (final RuntimeException e) {
            return Optional.empty();
        }
    }

    @Override
    public JsonObject trackDelivery(final String deliveryId, final String senderId) {
        final JsonObject reply = requestReply(CH_TRACK_REQUESTS,
                dyn("delivery-", deliveryId, "-tracking-requests-approved"),
                dyn("delivery-", deliveryId, "-tracking-requests-rejected"),
                new JsonObject().put("deliveryId", deliveryId).put("senderId", senderId));
        if (isApproved(reply)) {
            final JsonObject body = clean(reply);
            final String trackingSessionId = body.getString("trackingSessionId");
            if (trackingSessionId != null) {
                sessionToDelivery.put(trackingSessionId, deliveryId);
            }
            return body.put("_statusCode", 201);
        }
        return rejectedBody(reply, mapTrackStatus(reply.getString("errorType")));
    }

    /** Non nell'interfaccia/endpoint: sara' usato dal bridge WS (STEP 5). */
    public JsonObject stopTracking(final String deliveryId, final String senderId) {
        final JsonObject reply = requestReply(CH_STOP_TRACKING_REQUESTS,
                dyn("stop-tracking-", deliveryId, "-requests-approved"),
                dyn("stop-tracking-", deliveryId, "-requests-rejected"),
                new JsonObject().put("deliveryId", deliveryId).put("senderId", senderId));
        return isApproved(reply)
                ? clean(reply).put("_statusCode", 200)
                : rejectedBody(reply, mapTrackStatus(reply.getString("errorType")));
    }

    // ---- Admin via HTTP verso la porta 9003 (INVARIATO) ----
    @Override
    public JsonObject viewFleet() {
        return httpGetArray(fleetPort, "/api/v1/admin/fleet", "fleet");
    }

    @Override
    public JsonObject viewScheduling(final String droneId) {
        String path = "/api/v1/admin/scheduling";
        if (droneId != null && !droneId.isBlank()) {
            path += "?droneId=" + droneId;
        }
        return httpGetArray(fleetPort, path, "scheduling");
    }

    // ---- Bridge tracking Kafka -> EventBus (usato dal controller allo STEP 5) ----
    public InputEventChannel createAnEventChannel(final Vertx vertx, final String deliveryId,
                                                  final String trackingSessionId) {
        final InputEventChannel channel = new InputEventChannel(
                vertx, "delivery-tracking-" + deliveryId + "-external-events", address);
        channel.init(event -> vertx.eventBus().publish(trackingSessionId, event));
        return channel;
    }

    public Optional<String> deliveryIdFor(final String trackingSessionId) {
        return Optional.ofNullable(sessionToDelivery.get(trackingSessionId));
    }

    public void forgetTrackingSession(final String trackingSessionId) {
        sessionToDelivery.remove(trackingSessionId);
    }

    // ---- request/reply Kafka ----
    private JsonObject requestReply(final String inputChannel, final String approvedChannel,
                                    final String rejectedChannel, final JsonObject payload) {
        final String requestId = UUID.randomUUID().toString();
        payload.put("requestId", requestId);
        final Promise<JsonObject> promise = Promise.promise();
        pending.put(requestId, promise);
        // Sottoscrivi le risposte PRIMA di postare (earliest copre eventuali race sul rebalance).
        Future.all(ensureSubscribed(approvedChannel, true), ensureSubscribed(rejectedChannel, false))
                .onComplete(ar -> out(inputChannel).postEvent(payload));
        try {
            return promise.future().toCompletionStage().toCompletableFuture()
                    .get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final TimeoutException e) {
            pending.remove(requestId);
            throw new RuntimeException("delivery-service did not respond within "
                    + REQUEST_TIMEOUT_SECONDS + "s");
        } catch (final InterruptedException e) {
            pending.remove(requestId);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while contacting delivery-service", e);
        } catch (final ExecutionException e) {
            pending.remove(requestId);
            throw new RuntimeException("Failed to contact delivery-service", e);
        }
    }

    private Future<Void> ensureSubscribed(final String channel, final boolean approved) {
        if (subscriptions.containsKey(channel)) {
            return Future.succeededFuture();
        }
        final InputEventChannel in = new InputEventChannel(vertx, channel, address);
        subscriptions.put(channel, in);
        return in.init(approved ? this::completeApproved : this::completeRejected);
    }

    private void completeApproved(final JsonObject ev) {
        complete(ev, "approved");
    }

    private void completeRejected(final JsonObject ev) {
        complete(ev, "rejected");
    }

    private void complete(final JsonObject ev, final String outcome) {
        final Promise<JsonObject> promise = pending.remove(ev.getString("requestId"));
        if (promise != null) {
            promise.complete(ev.put("__outcome", outcome));
        }
    }

    private OutputEventChannel out(final String channel) {
        return outChannels.computeIfAbsent(channel, c -> new OutputEventChannel(vertx, c, address));
    }

    // ---- Admin HTTP helper ----
    private JsonObject httpGetArray(final int p, final String path, final String key) {
        final CompletableFuture<JsonObject> future = new CompletableFuture<>();
        webClient.get(p, host, path).timeout(ADMIN_TIMEOUT_MS).send(ar -> {
            if (ar.succeeded()) {
                future.complete(new JsonObject().put(key, ar.result().bodyAsJsonArray()));
            } else {
                future.completeExceptionally(ar.cause());
            }
        });
        try {
            return future.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while contacting delivery admin", e);
        } catch (final ExecutionException e) {
            throw new RuntimeException("Failed to contact delivery admin", e);
        }
    }

    // ---- Helpers ----
    private static boolean isApproved(final JsonObject reply) {
        return "approved".equals(reply.getString("__outcome"));
    }

    private static JsonObject clean(final JsonObject reply) {
        final JsonObject c = reply.copy();
        c.remove("__outcome");
        c.remove("requestId");
        c.remove("timestamp");
        return c;
    }

    private static JsonObject rejectedBody(final JsonObject reply, final int statusCode) {
        return new JsonObject()
                .put("error", reply.getString("reason", "Delivery request rejected"))
                .put("_statusCode", statusCode);
    }

    private static String dyn(final String prefix, final String id, final String suffix) {
        return prefix + id + suffix;
    }

    private static int mapCreateStatus(final String errorType) {
        if ("VALIDATION_REJECTED".equals(errorType)) {
            return 422;
        }
        if ("BAD_REQUEST".equals(errorType)) {
            return 400;
        }
        return 500;
    }

    private static int mapCancelStatus(final String errorType) {
        if ("CANNOT_CANCEL_IN_FLIGHT".equals(errorType) || "CONFLICT".equals(errorType)) {
            return 409;
        }
        if ("NOT_FOUND".equals(errorType)) {
            return 404;
        }
        return 500;
    }

    private static int mapTrackStatus(final String errorType) {
        if ("NOT_FOUND".equals(errorType)) {
            return 404;
        }
        return 500;
    }
}
