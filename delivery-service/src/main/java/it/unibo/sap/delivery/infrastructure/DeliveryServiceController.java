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
import it.unibo.sap.delivery.application.DeliveryExceptions.ForbiddenDeliveryAccessException;
import it.unibo.sap.delivery.application.DeliveryExceptions.ForbiddenException;
import it.unibo.sap.delivery.application.DeliveryExceptions.UnauthorizedException;
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

    private static final String ROLE_SENDER = "SENDER";

    // Canali statici
    private static final String CH_CREATE_REQUESTS = "create-delivery-requests";
    private static final String CH_CREATE_APPROVED = "create-delivery-requests-approved";
    private static final String CH_CREATE_REJECTED = "create-delivery-requests-rejected";
    private static final String CH_NEW_DELIVERY_CREATED = "new-delivery-created";
    private static final String CH_CANCEL_REQUESTS = "cancel-delivery-requests";
    private static final String CH_GET_DETAIL_REQUESTS = "get-delivery-detail-requests";
    private static final String CH_TRACK_REQUESTS = "delivery-tracking-requests";
    private static final String CH_STOP_TRACKING_REQUESTS = "stop-tracking-requests";

    private final DeliveryService deliveryService;
    private final RequestAuthorizer authorizer;
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

    public DeliveryServiceController(final DeliveryService deliveryService, final RequestAuthorizer authorizer,
                                     final int port, final String eventChannelsLocation) {
        this.deliveryService = deliveryService;
        this.authorizer = authorizer;
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

    private String resolveCaller(final JsonObject req, final String requiredRole) {
        final RequestAuthorizer.AuthResult result = authorizer.authorize(req.getString("sessionId"), requiredRole);
        if (result instanceof RequestAuthorizer.AuthResult.Authorized ok) {
            return ok.accountId();
        }
        final RequestAuthorizer.AuthResult.Rejected rejected = (RequestAuthorizer.AuthResult.Rejected) result;
        if (rejected.httpStatus() == 403) {
            throw new ForbiddenException(rejected.reason());
        }
        throw new UnauthorizedException(rejected.reason());
    }

    private record CreatedOutcome(CreateDeliveryResult result, String senderId) { }

    private void onCreateRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        commandExecutor.<CreatedOutcome>executeBlocking(() -> {
                    final String senderId = resolveCaller(req, ROLE_SENDER);
                    final CreateDeliveryCommand cmd = toCommand(req, senderId);
                    return new CreatedOutcome(deliveryService.createDelivery(cmd), senderId);
                }, true)
                .onSuccess(outcome -> {
                    final CreateDeliveryResult result = outcome.result();
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
                            .put("senderId", outcome.senderId()));
                })
                .onFailure(e -> createRejected.postEvent(rejection(requestId, e.getMessage(), createErrorType(e))));
    }

    private void onGetRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        commandExecutor.<Optional<DeliveryTrackingView>>executeBlocking(() -> {
                    final String senderId = resolveCaller(req, ROLE_SENDER);
                    return deliveryService.getDelivery(deliveryId, senderId);
                }, true)
                .onSuccess(opt -> opt.ifPresentOrElse(
                        view -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-approved"))
                                .postEvent(toJson(view).put("requestId", requestId)),
                        () -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-rejected"))
                                .postEvent(rejection(requestId, deliveryId, "Delivery not found", "NOT_FOUND"))))
                .onFailure(e -> out(dynamic("get-delivery-", deliveryId, "-detail-requests-rejected"))
                        .postEvent(rejection(requestId, deliveryId, e.getMessage(), simpleErrorType(e))));
    }

    private void onCancelRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        commandExecutor.<Void>executeBlocking(() -> {
                    final String senderId = resolveCaller(req, ROLE_SENDER);
                    deliveryService.cancelDelivery(deliveryId, senderId);
                    return null;
                }, true)
                .onSuccess(v -> out(dynamic("cancel-delivery-", deliveryId, "-requests-approved"))
                        .postEvent(new JsonObject()
                                .put("requestId", requestId)
                                .put("deliveryId", deliveryId)
                                .put("status", "CANCELLED")))
                .onFailure(e -> out(dynamic("cancel-delivery-", deliveryId, "-requests-rejected"))
                        .postEvent(rejection(requestId, deliveryId, e.getMessage(), cancelErrorType(e))));
    }

    private void onTrackRequest(final JsonObject req) {
        final String requestId = req.getString("requestId");
        final String deliveryId = req.getString("deliveryId");
        commandExecutor.<TrackingHandle>executeBlocking(() -> {
                    final String senderId = resolveCaller(req, ROLE_SENDER);
                    return deliveryService.startTracking(deliveryId, senderId);
                }, true)
                .onSuccess(handle -> setupForwarder(deliveryId).onComplete(ar ->
                        out(dynamic("delivery-", deliveryId, "-tracking-requests-approved"))
                                .postEvent(new JsonObject()
                                        .put("requestId", requestId)
                                        .put("deliveryId", deliveryId)
                                        .put("trackingSessionId", handle.trackingSessionId()))))
                .onFailure(e -> out(dynamic("delivery-", deliveryId, "-tracking-requests-rejected"))
                        .postEvent(rejection(requestId, deliveryId, e.getMessage(), simpleErrorType(e))));
    }

    private void onStopTrackingRequest(final JsonObject req) {
        commandExecutor.<String>executeBlocking(() -> resolveCaller(req, ROLE_SENDER), true)
                .onSuccess(senderId -> doStopTracking(req))
                .onFailure(e -> {
                    final String requestId = req.getString("requestId");
                    final String deliveryId = req.getString("deliveryId");
                    out(dynamic("stop-tracking-", deliveryId, "-requests-rejected"))
                            .postEvent(rejection(requestId, deliveryId, e.getMessage(), simpleErrorType(e)));
                });
    }

    private void doStopTracking(final JsonObject req) {
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


    private static String authErrorType(final Throwable e) {
        if (e instanceof UnauthorizedException) {
            return "UNAUTHORIZED";
        }
        if (e instanceof ForbiddenException) {
            return "FORBIDDEN";
        }
        return null;
    }

    private static String createErrorType(final Throwable e) {
        final String auth = authErrorType(e);
        if (auth != null) {
            return auth;
        }
        if (e instanceof ValidationRejectedException) {
            return "VALIDATION_REJECTED";
        }
        if (e instanceof BadRequestException || e instanceof IllegalArgumentException) {
            return "BAD_REQUEST";
        }
        return "INTERNAL";
    }

    private static String cancelErrorType(final Throwable e) {
        final String auth = authErrorType(e);
        if (auth != null) {
            return auth;
        }
        if (e instanceof ForbiddenDeliveryAccessException) {
            return "FORBIDDEN";
        }
        if (e instanceof CannotCancelInFlightException) {
            return "CANNOT_CANCEL_IN_FLIGHT";
        }
        if (e instanceof DeliveryNotFoundException) {
            return "NOT_FOUND";
        }
        if (e instanceof IllegalStateException) {
            return "CONFLICT";
        }
        return "INTERNAL";
    }

    private static String simpleErrorType(final Throwable e) {
        final String auth = authErrorType(e);
        if (auth != null) {
            return auth;
        }
        return e instanceof DeliveryNotFoundException ? "NOT_FOUND" : "INTERNAL";
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


    private CreateDeliveryCommand toCommand(final JsonObject body, final String senderId) {
        if (body == null) {
            throw new BadRequestException("Missing request body");
        }
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
