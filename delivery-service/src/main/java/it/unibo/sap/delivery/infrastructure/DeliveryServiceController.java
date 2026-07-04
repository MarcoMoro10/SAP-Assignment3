package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import it.unibo.sap.common.hexagonal.InputAdapter;
import it.unibo.sap.delivery.application.CreateDeliveryCommand;
import it.unibo.sap.delivery.application.CreateDeliveryResult;
import it.unibo.sap.delivery.application.DeliveryExceptions.BadRequestException;
import it.unibo.sap.delivery.application.DeliveryExceptions.CannotCancelInFlightException;
import it.unibo.sap.delivery.application.DeliveryExceptions.DeliveryNotFoundException;
import it.unibo.sap.delivery.application.DeliveryExceptions.ValidationRejectedException;
import it.unibo.sap.delivery.application.DeliveryService;
import it.unibo.sap.delivery.application.TrackingHandle;
import it.unibo.sap.delivery.domain.deliveries.DeliveryTrackingView;
import it.unibo.sap.delivery.domain.deliveries.EstimatedTimeRemaining;
import it.unibo.sap.delivery.kafka.InputEventChannel;
import it.unibo.sap.delivery.kafka.OutputEventChannel;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class DeliveryServiceController extends AbstractVerticle implements InputAdapter {

    public static final String DOMAIN_COMMAND_EXECUTOR = "delivery-domain-commands";

    // --- Canali statici (nome = topic Kafka) ---
    private static final String CH_CREATE_REQUESTS = "create-delivery-requests";
    private static final String CH_CREATE_APPROVED = "create-delivery-requests-approved";
    private static final String CH_CREATE_REJECTED = "create-delivery-requests-rejected";
    private static final String CH_NEW_DELIVERY_CREATED = "new-delivery-created";
    private static final String CH_CANCEL_REQUESTS = "cancel-delivery-requests";
    private static final String CH_GET_DETAIL_REQUESTS = "get-delivery-detail-requests";
    private static final String CH_TRACK_REQUESTS = "delivery-tracking-requests";
    private static final String CH_STOP_TRACKING_REQUESTS = "stop-tracking-requests";

    private final DeliveryService deliveryService;
    private final int port;
    private final String eventChannelsLocation;

    private WorkerExecutor commandExecutor;

    // Output statici + cache dei canali dinamici ({id}) per riusare i producer.
    private OutputEventChannel createApproved;
    private OutputEventChannel createRejected;
    private OutputEventChannel newDeliveryCreated;
    private final Map<String, OutputEventChannel> dynamicOut = new ConcurrentHashMap<>();

    // Input statici.
    private InputEventChannel createIn;
    private InputEventChannel cancelIn;
    private InputEventChannel getIn;
    private InputEventChannel trackIn;
    private InputEventChannel stopTrackingIn;

    private final Map<String, InputEventChannel> trackingForwarders = new ConcurrentHashMap<>();

    public DeliveryServiceController(final DeliveryService deliveryService, final int port,
                                     final String eventChannelsLocation) {
        this.deliveryService = deliveryService;
        this.port = port;
        this.eventChannelsLocation = eventChannelsLocation;
    }

    @Override
    public void start(final Promise<Void> startPromise) {
        commandExecutor = vertx.createSharedWorkerExecutor(DOMAIN_COMMAND_EXECUTOR, 1);

        createApproved = new OutputEventChannel(vertx, CH_CREATE_APPROVED, eventChannelsLocation);
        createRejected = new OutputEventChannel(vertx, CH_CREATE_REJECTED, eventChannelsLocation);
        newDeliveryCreated = new OutputEventChannel(vertx, CH_NEW_DELIVERY_CREATED, eventChannelsLocation);
        createIn = new InputEventChannel(vertx, CH_CREATE_REQUESTS, eventChannelsLocation);
        cancelIn = new InputEventChannel(vertx, CH_CANCEL_REQUESTS, eventChannelsLocation);
        getIn = new InputEventChannel(vertx, CH_GET_DETAIL_REQUESTS, eventChannelsLocation);
        trackIn = new InputEventChannel(vertx, CH_TRACK_REQUESTS, eventChannelsLocation);
        stopTrackingIn = new InputEventChannel(vertx, CH_STOP_TRACKING_REQUESTS, eventChannelsLocation);

        final Router router = Router.router(vertx);
        router.get("/api/v1/health").handler(this::handleHealth);
        final var server = vertx.createHttpServer();

        final Future<Void> httpReady = server.requestHandler(router).listen(port).mapEmpty();
        final Future<Void> kafkaReady = Future.all(
                createIn.init(this::onCreateRequest),
                cancelIn.init(this::onCancelRequest),
                getIn.init(this::onGetRequest),
                trackIn.init(this::onTrackRequest),
                stopTrackingIn.init(this::onStopTrackingRequest)).mapEmpty();

        Future.all(httpReady, kafkaReady)
                .onSuccess(v -> {
                    System.out.println("delivery-service ready - port: " + port
                            + " (Kafka commands @ " + eventChannelsLocation + ")");
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    @Override
    public void stop() {
        if (commandExecutor != null) {
            commandExecutor.close();
        }
        closeQuietly(createIn);
        closeQuietly(cancelIn);
        closeQuietly(getIn);
        closeQuietly(trackIn);
        closeQuietly(stopTrackingIn);
        trackingForwarders.values().forEach(this::closeQuietly);
    }

    private void closeQuietly(final InputEventChannel channel) {
        if (channel != null) {
            channel.close();
        }
    }

    private void onCreateRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final CreateDeliveryCommand cmd;
        try {
            cmd = toCommand(req);
        } catch (final BadRequestException | IllegalArgumentException e) {
            createRejected.postEvent(rejection(requestId, e.getMessage(), "BAD_REQUEST"));
            return;
        }
        commandExecutor.<CreateDeliveryResult>executeBlocking(
                        () -> deliveryService.createDelivery(cmd), true)
                .onSuccess(result -> {
                    final JsonObject approved = new JsonObject()
                            .put("requestId", requestId)
                            .put("deliveryId", result.deliveryId())
                            .put("status", result.status());
                    if (result.assignedDroneId() != null) {
                        approved.put("assignedDroneId", result.assignedDroneId());
                    }
                    createApproved.postEvent(approved);
                    newDeliveryCreated.postEvent(new JsonObject()
                            .put("deliveryId", result.deliveryId())
                            .put("status", result.status())
                            .put("senderId", cmd.senderId()));
                })
                .onFailure(e -> {
                    final String errorType;
                    if (e instanceof ValidationRejectedException) {
                        errorType = "VALIDATION_REJECTED";
                    } else if (e instanceof BadRequestException || e instanceof IllegalArgumentException) {
                        errorType = "BAD_REQUEST";
                    } else {
                        errorType = "INTERNAL";
                    }
                    createRejected.postEvent(rejection(requestId, e.getMessage(), errorType));
                });
    }

    private void onGetRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        final String senderId = req.getString("senderId");
        commandExecutor.<Optional<DeliveryTrackingView>>executeBlocking(
                        () -> deliveryService.getDelivery(deliveryId, senderId), true)
                .onSuccess(opt -> opt.ifPresentOrElse(
                        view -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-approved"))
                                .postEvent(toJson(view).put("requestId", requestId)),
                        () -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-rejected"))
                                .postEvent(rejection(requestId, deliveryId, "Delivery not found", "NOT_FOUND"))))
                .onFailure(e -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-rejected"))
                        .postEvent(rejection(requestId, deliveryId, e.getMessage(), "INTERNAL")));
    }

    private void onCancelRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        final String senderId = req.getString("senderId");
        commandExecutor.<Void>executeBlocking(() -> {
                    deliveryService.cancelDelivery(deliveryId, senderId);
                    return null;
                }, true)
                .onSuccess(v -> out(dynamic("cancel-delivery-", deliveryId, "-requests-approved"))
                        .postEvent(new JsonObject()
                                .put("requestId", requestId)
                                .put("deliveryId", deliveryId)
                                .put("status", "CANCELLED")))
                .onFailure(e -> {
                    final String errorType;
                    if (e instanceof CannotCancelInFlightException) {
                        errorType = "CANNOT_CANCEL_IN_FLIGHT";
                    } else if (e instanceof DeliveryNotFoundException) {
                        errorType = "NOT_FOUND";
                    } else if (e instanceof IllegalStateException) {
                        errorType = "CONFLICT";
                    } else {
                        errorType = "INTERNAL";
                    }
                    out(dynamic("cancel-delivery-", deliveryId, "-requests-rejected"))
                            .postEvent(rejection(requestId, deliveryId, e.getMessage(), errorType));
                });
    }

    private void onTrackRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        final String senderId = req.getString("senderId");
        commandExecutor.<TrackingHandle>executeBlocking(
                        () -> deliveryService.startTracking(deliveryId, senderId), true)
                .onSuccess(handle -> setupForwarder(deliveryId).onComplete(ar ->
                        out(dynamic("delivery-", deliveryId, "-tracking-requests-approved"))
                                .postEvent(new JsonObject()
                                        .put("requestId", requestId)
                                        .put("deliveryId", deliveryId)
                                        .put("trackingSessionId", handle.trackingSessionId()))))
                .onFailure(e -> {
                    final String errorType = e instanceof DeliveryNotFoundException ? "NOT_FOUND" : "INTERNAL";
                    out(dynamic("delivery-", deliveryId, "-tracking-requests-rejected"))
                            .postEvent(rejection(requestId, deliveryId, e.getMessage(), errorType));
                });
    }

    private void onStopTrackingRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        final InputEventChannel forwarder = trackingForwarders.remove(deliveryId);
        if (forwarder == null) {
            out(dynamic("stop-tracking-", deliveryId, "-requests-rejected"))
                    .postEvent(rejection(requestId, deliveryId, "No active tracking session", "NOT_FOUND"));
            return;
        }
        forwarder.close();
        out(dynamic("delivery-tracking-", deliveryId, "-external-events"))
                .postEvent(new JsonObject().put("event", "TRACKING_CLOSED").put("deliveryId", deliveryId));
        out(dynamic("stop-tracking-", deliveryId, "-requests-approved"))
                .postEvent(new JsonObject()
                        .put("requestId", requestId)
                        .put("deliveryId", deliveryId)
                        .put("status", "STOPPED"));
    }

    private Future<Void> setupForwarder(final String deliveryId) {
        if (trackingForwarders.containsKey(deliveryId)) {
            return Future.succeededFuture();
        }
        final InputEventChannel internalIn = new InputEventChannel(
                vertx, dynamic("delivery-tracking-", deliveryId, "-internal-events"), eventChannelsLocation);
        trackingForwarders.put(deliveryId, internalIn);
        final OutputEventChannel externalOut = out(dynamic("delivery-tracking-", deliveryId, "-external-events"));
        return internalIn.init(externalOut::postEvent);
    }

    private static String dynamic(final String prefix, final String id, final String suffix) {
        return prefix + id + suffix;
    }

    private OutputEventChannel out(final String channelName) {
        return dynamicOut.computeIfAbsent(channelName,
                n -> new OutputEventChannel(vertx, n, eventChannelsLocation));
    }

    private static JsonObject rejection(final String requestId, final String reason, final String errorType) {
        return new JsonObject()
                .put("requestId", requestId)
                .put("reason", reason)
                .put("errorType", errorType);
    }

    private static JsonObject rejection(final String requestId, final String deliveryId,
                                        final String reason, final String errorType) {
        return rejection(requestId, reason, errorType).put("deliveryId", deliveryId);
    }

    private void handleHealth(final RoutingContext ctx) {
        ctx.response().setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject().put("status", "UP").encode());
    }


    private CreateDeliveryCommand toCommand(final JsonObject body) {
        if (body == null) {
            throw new BadRequestException("Missing request body");
        }
        final String senderId = body.getString("senderId");
        final double weight = body.getDouble("weight", 0.0);
        final JsonObject start = body.getJsonObject("startingPlace");
        final JsonObject dest = body.getJsonObject("destinationPlace");
        if (start == null || dest == null) {
            throw new BadRequestException("Invalid address");
        }
        final boolean immediate = body.getBoolean("immediate", true);
        final String scheduledAtRaw = body.getString("scheduledAt");
        final LocalDateTime scheduledAt = scheduledAtRaw == null ? null : parseDateTime(scheduledAtRaw);
        final long deadlineMinutes = body.getLong("deadlineMinutes", 0L);
        if (deadlineMinutes <= 0) {
            throw new BadRequestException("deadlineMinutes is required and must be greater than 0");
        }
        return new CreateDeliveryCommand(
                senderId, weight,
                start.getString("street"), start.getInteger("number", 0),
                dest.getString("street"), dest.getInteger("number", 0),
                immediate, scheduledAt, deadlineMinutes);
    }

    private LocalDateTime parseDateTime(final String raw) {
        try {
            return java.time.OffsetDateTime.parse(raw).toLocalDateTime();
        } catch (final Exception ignored) {
            try {
                return LocalDateTime.parse(raw);
            } catch (final Exception e) {
                throw new BadRequestException("Invalid shipping time");
            }
        }
    }

    private JsonObject toJson(final DeliveryTrackingView v) {
        return new JsonObject()
                .put("deliveryId", v.deliveryId())
                .put("status", v.status().name())
                .put("estimatedTimeRemainingSeconds", v.estimatedTimeRemainingSeconds())
                .put("estimatedTimeRemainingFormatted",
                        EstimatedTimeRemaining.formatSeconds(v.estimatedTimeRemainingSeconds()));
    }
}
