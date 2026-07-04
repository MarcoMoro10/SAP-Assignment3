package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import it.unibo.sap.common.hexagonal.InputAdapter;
import it.unibo.sap.delivery.application.CreateDeliveryCommand;
import it.unibo.sap.delivery.application.CreateDeliveryResult;
import it.unibo.sap.delivery.application.DeliveryExceptions.BadRequestException;
import it.unibo.sap.delivery.application.DeliveryExceptions.CannotCancelInFlightException;
import it.unibo.sap.delivery.application.DeliveryExceptions.DeliveryNotFoundException;
import it.unibo.sap.delivery.application.DeliveryExceptions.ValidationRejectedException;
import it.unibo.sap.delivery.application.DeliveryService;
import it.unibo.sap.delivery.application.TrackingHandle;
import it.unibo.sap.delivery.domain.deliveries.DeliveryStatus;
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

        final Router router = Router.router(vertx);
        router.route("/api/v1/*").handler(BodyHandler.create());
        router.get("/api/v1/health").handler(this::handleHealth);
        router.post("/api/v1/deliveries/:deliveryId/track").handler(this::handleTrack);

        final var server = vertx.createHttpServer();
        server.webSocketHandler(this::handleTrackingSocket);

        final Future<Void> httpReady = server.requestHandler(router).listen(port).mapEmpty();
        final Future<Void> kafkaReady = Future.all(
                createIn.init(this::onCreateRequest),
                cancelIn.init(this::onCancelRequest),
                getIn.init(this::onGetRequest)).mapEmpty();

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
        if (createIn != null) {
            createIn.close();
        }
        if (cancelIn != null) {
            cancelIn.close();
        }
        if (getIn != null) {
            getIn.close();
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

    private void handleTrack(final RoutingContext ctx) {
        final String deliveryId = ctx.pathParam("deliveryId");
        final JsonObject body = ctx.body().asJsonObject();
        final String senderId = body == null ? null : body.getString("senderId");
        try {
            final TrackingHandle handle = deliveryService.startTracking(deliveryId, senderId);
            final String wsUrl = "ws://localhost:" + port + "/api/v1/track/" + handle.trackingSessionId();
            ctx.response().setStatusCode(201)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject()
                            .put("trackingSessionId", handle.trackingSessionId())
                            .put("deliveryId", handle.deliveryId())
                            .put("webSocketUrl", wsUrl).encode());
        } catch (final DeliveryNotFoundException e) {
            error(ctx, 404, e.getMessage());
        }
    }

    private void handleTrackingSocket(final io.vertx.core.http.ServerWebSocket webSocket) {
        if (!webSocket.path().startsWith("/api/v1/track/")) {
            webSocket.reject();
            return;
        }
        webSocket.textMessageHandler(openMsg -> {
            if (openMsg == null || openMsg.isBlank()) {
                return;
            }
            final JsonObject obj;
            try {
                obj = new JsonObject(openMsg);
            } catch (final RuntimeException e) {
                webSocket.writeTextMessage(new JsonObject()
                        .put("error", "Expected JSON {\"deliveryId\":\"...\"}").encode());
                return;
            }
            final String deliveryId = obj.getString("deliveryId");
            if (deliveryId == null || deliveryId.isBlank()) {
                webSocket.writeTextMessage(new JsonObject()
                        .put("error", "Missing deliveryId").encode());
                return;
            }
            final String address = VertxTrackingSessionEventObserver.TRACKING_ADDRESS_PREFIX + deliveryId;
            vertx.eventBus().consumer(address, msg -> {
                final JsonObject update = (JsonObject) msg.body();
                final String frame = update.encode();
                if (isTerminalStatus(update.getString("status"))) {
                    webSocket.writeTextMessage(frame, ar -> webSocket.close());
                } else {
                    webSocket.writeTextMessage(frame);
                }
            });
        });
    }

    private static boolean isTerminalStatus(final String status) {
        return DeliveryStatus.DELIVERED.name().equals(status);
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

    private void error(final RoutingContext ctx, final int status, final String message) {
        ctx.response().setStatusCode(status)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject().put("error", message).encode());
    }
}
