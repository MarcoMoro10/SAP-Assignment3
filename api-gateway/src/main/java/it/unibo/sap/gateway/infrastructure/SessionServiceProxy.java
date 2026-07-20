package it.unibo.sap.gateway.infrastructure;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import it.unibo.sap.common.hexagonal.OutputAdapter;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class SessionServiceProxy implements OutputAdapter {

    private static final String SESSIONS_PATH = "/api/v1/user-sessions/";

    private static final long HEALTH_TIMEOUT_MS = 2000;
    private static final long INTROSPECT_TIMEOUT_MS = 2000;   // WS auth must be snappy; the timeout is the defense
    private static final long LOGIN_TIMEOUT_MS = 10_000;

    private final WebClient webClient;
    private final String host;
    private final int port;

    public SessionServiceProxy(final WebClient webClient, final String host, final int port) {
        this.webClient = webClient;
        this.host = host;
        this.port = port;
    }

    public Future<Boolean> pingHealth() {
        return webClient.get(port, host, "/api/v1/health")
                .timeout(HEALTH_TIMEOUT_MS)
                .send()
                .map(resp -> resp.statusCode() == 200)
                .otherwise(false);
    }

    public JsonObject login(final String username, final String password) {
        final CompletableFuture<JsonObject> future = new CompletableFuture<>();
        final JsonObject body = new JsonObject().put("username", username).put("password", password);
        webClient.post(port, host, "/api/v1/login")
                .timeout(LOGIN_TIMEOUT_MS)
                .sendJsonObject(body, ar -> {
                    if (ar.succeeded()) {
                        future.complete(bodyWithStatus(ar.result()));
                    } else {
                        future.complete(unreachable());
                    }
                });
        try {
            return future.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return unreachable();
        } catch (final Exception e) {
            return unreachable();
        }
    }

    public Optional<ValidatedCaller> introspect(final String sessionId) {
        if (sessionId == null || sessionId.isBlank()) {
            return Optional.empty();
        }
        final CompletableFuture<Optional<ValidatedCaller>> future = new CompletableFuture<>();
        webClient.get(port, host, SESSIONS_PATH + sessionId)
                .timeout(INTROSPECT_TIMEOUT_MS)
                .send(ar -> {
                    if (ar.succeeded() && ar.result().statusCode() == 200) {
                        final JsonObject b = ar.result().bodyAsJsonObject();
                        future.complete(Optional.of(new ValidatedCaller(b.getString("accountId"), b.getString("role"))));
                    } else {
                        future.complete(Optional.empty());
                    }
                });
        try {
            return future.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (final Exception e) {
            return Optional.empty();
        }
    }

    public record ValidatedCaller(String accountId, String role) { }

    private static JsonObject unreachable() {
        return new JsonObject().put("_statusCode", 502).put("error", "session-service unreachable");
    }

    private static JsonObject bodyWithStatus(final HttpResponse<Buffer> resp) {
        JsonObject body;
        try {
            body = resp.bodyAsJsonObject();
        } catch (final RuntimeException notJson) {
            body = null;
        }
        if (body == null) {
            body = new JsonObject();
        }
        return body.put("_statusCode", resp.statusCode());
    }
}
