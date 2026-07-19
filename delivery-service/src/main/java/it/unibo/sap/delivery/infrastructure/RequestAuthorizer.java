package it.unibo.sap.delivery.infrastructure;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import it.unibo.sap.delivery.application.SessionValidatorPort;
import it.unibo.sap.delivery.application.SessionValidatorPort.ValidatedCaller;

import java.util.Optional;

public class RequestAuthorizer {

    static final String SESSION_ID_HEADER = "X-Session-Id";

    public static final String CALLER_ACCOUNT_ID = "callerAccountId";

    private final SessionValidatorPort sessionValidator;

    public RequestAuthorizer(final SessionValidatorPort sessionValidator) {
        this.sessionValidator = sessionValidator;
    }

    public AuthResult authorize(final String sessionId, final String requiredRole) {
        if (sessionId == null || sessionId.isBlank()) {
            return new AuthResult.Rejected(401, "UNAUTHORIZED", "Missing session identity");
        }
        final Optional<ValidatedCaller> caller = sessionValidator.validate(sessionId);
        if (caller.isEmpty()) {
            return new AuthResult.Rejected(401, "UNAUTHORIZED", "Invalid or expired session");
        }
        if (!requiredRole.equals(caller.get().role())) {
            return new AuthResult.Rejected(403, "FORBIDDEN", "Forbidden: requires " + requiredRole + " role");
        }
        return new AuthResult.Authorized(caller.get().accountId());
    }

    public sealed interface AuthResult {
        record Authorized(String accountId) implements AuthResult { }

        record Rejected(int httpStatus, String errorType, String reason) implements AuthResult { }
    }

    public Handler<RoutingContext> requireRole(final String requiredRole) {
        return ctx -> {
            final String sessionId = ctx.request().getHeader(SESSION_ID_HEADER);
            // TRANSITIONAL (STEP 5): the gateway does not yet propagate identity on the admin routes,
            // so a missing header is a legacy pass-through (keeps fleet/scheduling views working
            // end-to-end). STEP 6 makes this strict (A2 behaviour): remove this branch so a missing
            // header yields 401 "Missing session identity".
            if (sessionId == null || sessionId.isBlank()) {
                ctx.next();
                return;
            }
            final AuthResult result = authorize(sessionId, requiredRole);
            if (result instanceof AuthResult.Authorized ok) {
                ctx.put(CALLER_ACCOUNT_ID, ok.accountId());
                ctx.next();
            } else {
                final AuthResult.Rejected rejected = (AuthResult.Rejected) result;
                ctx.response().setStatusCode(rejected.httpStatus())
                        .putHeader("Content-Type", "application/json")
                        .end(new JsonObject().put("error", rejected.reason()).encode());
            }
        };
    }
}
