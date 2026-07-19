package it.unibo.sap.delivery.infrastructure;

import it.unibo.sap.delivery.infrastructure.RequestAuthorizer.AuthResult;
import it.unibo.sap.delivery.support.FakeSessionValidator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class RequestAuthorizerTest {

    private final RequestAuthorizer authorizer = new RequestAuthorizer(new FakeSessionValidator());

    @Test
    void missingSessionIsUnauthorized() {
        for (final String blank : new String[] {null, "", "   "}) {
            final AuthResult r = authorizer.authorize(blank, "SENDER");
            final AuthResult.Rejected rejected = assertInstanceOf(AuthResult.Rejected.class, r);
            assertEquals(401, rejected.httpStatus());
            assertEquals("UNAUTHORIZED", rejected.errorType());
            assertEquals("Missing session identity", rejected.reason());
        }
    }

    @Test
    void invalidSessionIsUnauthorized() {
        final AuthResult r = authorizer.authorize(FakeSessionValidator.INVALID_TOKEN, "SENDER");
        final AuthResult.Rejected rejected = assertInstanceOf(AuthResult.Rejected.class, r);
        assertEquals(401, rejected.httpStatus());
        assertEquals("UNAUTHORIZED", rejected.errorType());
        assertEquals("Invalid or expired session", rejected.reason());
    }

    @Test
    void aSenderIsForbiddenFromAnAdminOnlyAction() {
        final AuthResult r = authorizer.authorize("user-1", "ADMIN");
        final AuthResult.Rejected rejected = assertInstanceOf(AuthResult.Rejected.class, r);
        assertEquals(403, rejected.httpStatus());
        assertEquals("FORBIDDEN", rejected.errorType());
        assertEquals("Forbidden: requires ADMIN role", rejected.reason());
    }

    @Test
    void aValidSessionWithTheRequiredRoleIsAuthorizedAndYieldsTheAccountId() {
        assertEquals(new AuthResult.Authorized("user-1"), authorizer.authorize("user-1", "SENDER"));
        assertEquals(new AuthResult.Authorized("admin-1"), authorizer.authorize("admin-1", "ADMIN"));
    }
}
