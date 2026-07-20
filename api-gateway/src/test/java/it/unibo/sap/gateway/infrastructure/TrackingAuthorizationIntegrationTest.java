package it.unibo.sap.gateway.infrastructure;

import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Authorization of the gateway tracking WebSocket (STEP 6). The gateway is the only point that sees
 * the WS handshake (the stream never crosses Kafka), so it authorizes the OWNER there, at the upgrade,
 * before any relay channel is opened. The owner of a tracking session is seeded deterministically (the
 * real capture path via {@code trackDelivery} is covered by {@link DeliveryServiceProxyIntegrationTest});
 * this class focuses on the gate. The core, non-negotiable assertion is that a rejected handshake NEVER
 * opens the Kafka relay channel ({@code createAnEventChannel} is not invoked).
 */
class TrackingAuthorizationIntegrationTest {

    private static final String HOST = "localhost";
    private static final int SESSION_STUB_PORT = 9420;
    private static final int GATEWAY_PORT = 9421;
    private static final String DELIVERY_ID = "DLV-7";
    private static final String TRACKING_SESSION_ID = "DLV-7-TRK";
    private static final String OWNER_SESSION = "sess-owner";        // -> acc-owner (the owner)
    private static final String INTRUDER_SESSION = "sess-intruder";  // -> acc-intruder (a valid non-owner)

    private static Vertx vertx;
    private static WebSocketClient wsClient;
    private static DeliveryServiceProxy deliveryProxy;

    @BeforeAll
    static void startSystem() {
        vertx = Vertx.vertx();
        startSessionStub();

        final WebClient webClient = WebClient.create(vertx);
        final SessionServiceProxy sessionProxy = new SessionServiceProxy(webClient, HOST, SESSION_STUB_PORT);
        deliveryProxy = new DeliveryServiceProxy(
                vertx, webClient, sessionProxy, HOST, SESSION_STUB_PORT, SESSION_STUB_PORT, "localhost:59092");
        deliveryProxy.rememberTrackedDelivery(TRACKING_SESSION_ID, DELIVERY_ID, "acc-owner");

        final CountDownLatch deployed = new CountDownLatch(1);
        vertx.deployVerticle(new APIGatewayController(
                        new AccountServiceProxy(webClient, HOST, SESSION_STUB_PORT),
                        deliveryProxy, sessionProxy, HOST, GATEWAY_PORT))
                .onComplete(ar -> deployed.countDown());
        await(deployed);

        wsClient = vertx.createWebSocketClient();
    }

    @AfterAll
    static void stopSystem() {
        if (vertx != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            vertx.close().onComplete(ar -> latch.countDown());
            await(latch);
        }
    }

    private static void startSessionStub() {
        final Router router = Router.router(vertx);
        router.route("/api/v1/*").handler(BodyHandler.create());
        router.get("/api/v1/user-sessions/:sessionId").handler(ctx -> {
            final String id = ctx.pathParam("sessionId");
            final String account = switch (id) {
                case OWNER_SESSION -> "acc-owner";
                case INTRUDER_SESSION -> "acc-intruder";
                default -> null;
            };
            if (account == null) {
                ctx.response().setStatusCode(404).end(new JsonObject().put("error", "Unknown session").encode());
            } else {
                ctx.response().setStatusCode(200).putHeader("Content-Type", "application/json")
                        .end(new JsonObject().put("accountId", account).put("role", "SENDER").encode());
            }
        });
        final CountDownLatch latch = new CountDownLatch(1);
        vertx.createHttpServer().requestHandler(router).listen(SESSION_STUB_PORT).onComplete(ar -> latch.countDown());
        await(latch);
    }

    @Test
    void aHandshakeWithoutIdentityIsRejectedAndNoKafkaChannelIsOpened() throws Exception {
        final int before = deliveryProxy.eventChannelsOpened.get();
        final CompletableFuture<Void> connected = new CompletableFuture<>();
        wsClient.connect(GATEWAY_PORT, HOST, "/api/v1/track/" + TRACKING_SESSION_ID)   // no ?sessionId
                .onSuccess(ws -> connected.complete(null))
                .onFailure(connected::completeExceptionally);

        assertTrue(rejected(connected), "a handshake without identity must be rejected at the upgrade");
        Thread.sleep(500);
        assertEquals(before, deliveryProxy.eventChannelsOpened.get(),
                "a rejected handshake must NOT open the Kafka relay channel");
    }

    @Test
    void aHandshakeFromANonOwnerIsClosedAndNoKafkaChannelIsOpened() throws Exception {
        final int before = deliveryProxy.eventChannelsOpened.get();
        final AtomicBoolean closed = new AtomicBoolean(false);
        final CompletableFuture<Void> done = new CompletableFuture<>();
        wsClient.connect(GATEWAY_PORT, HOST, "/api/v1/track/" + TRACKING_SESSION_ID + "?sessionId=" + INTRUDER_SESSION)
                .onSuccess(ws -> {
                    ws.closeHandler(v -> {
                        closed.set(true);
                        done.complete(null);
                    });
                    ws.writeTextMessage(new JsonObject().put("deliveryId", DELIVERY_ID).encode());
                })
                .onFailure(done::completeExceptionally);

        done.get(15, TimeUnit.SECONDS);
        assertTrue(closed.get(), "the non-owner socket must be closed by the gateway");
        Thread.sleep(500);
        assertEquals(before, deliveryProxy.eventChannelsOpened.get(),
                "a non-owner handshake must NOT open the Kafka relay channel");
    }

    @Test
    void theOwnerOpensExactlyOneRelayChannel() throws Exception {
        final int before = deliveryProxy.eventChannelsOpened.get();
        final CompletableFuture<Void> connected = new CompletableFuture<>();
        wsClient.connect(GATEWAY_PORT, HOST, "/api/v1/track/" + TRACKING_SESSION_ID + "?sessionId=" + OWNER_SESSION)
                .onSuccess(ws -> {
                    ws.writeTextMessage(new JsonObject().put("deliveryId", DELIVERY_ID).encode());
                    connected.complete(null);
                })
                .onFailure(connected::completeExceptionally);
        connected.get(15, TimeUnit.SECONDS);

        for (int i = 0; i < 50 && deliveryProxy.eventChannelsOpened.get() == before; i++) {
            Thread.sleep(100);
        }
        assertEquals(before + 1, deliveryProxy.eventChannelsOpened.get(),
                "the owner's authorized handshake opens the Kafka relay channel");
    }

    private static boolean rejected(final CompletableFuture<Void> connected) {
        try {
            connected.get(15, TimeUnit.SECONDS);
            return false;
        } catch (final Exception e) {
            return true;
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out setting up the tracking-authorization test system");
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while setting up the test system", e);
        }
    }
}
