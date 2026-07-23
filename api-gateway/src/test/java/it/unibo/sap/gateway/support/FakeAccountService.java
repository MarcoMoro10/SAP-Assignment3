package it.unibo.sap.gateway.support;

import io.vertx.core.json.JsonObject;
import it.unibo.sap.gateway.application.AccountService;

/**
 * Hand-written mock of the gateway's {@link AccountService} outbound port (the AccountServiceProxy)
 * for solitary unit tests of the session/orchestration logic.
 */
public final class FakeAccountService implements AccountService {

    private JsonObject registerResult =
            new JsonObject().put("_statusCode", 201).put("accountId", "acc-1").put("role", "SENDER");

    @Override
    public JsonObject register(final String username, final String password) {
        return registerResult;
    }
}
