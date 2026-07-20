package system.steps;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct-access security scenarios: they call the delivery-service admin endpoint on its OWN port
 * ({@link Setup#DELIVERY_ADMIN_PORT}), bypassing the gateway, to prove the delivery-service authorizes
 * by itself — security is defense-in-depth, not perimeter. Without a propagated identity the call is
 * 401; with a Sender identity (wrong role for an admin view) it is 403.
 */
public class DirectDeliveryAccessSteps {

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final AtomicInteger SEQ = new AtomicInteger();
    private static final String FLEET_PATH = "/api/v1/admin/fleet";

    private String senderSessionId;
    private int lastDirectStatus;

    @And("I have a valid Sender session")
    public void iHaveAValidSenderSession() {
        final String user = "direct-" + System.currentTimeMillis() + "-" + SEQ.incrementAndGet();
        final String creds = "{\"username\":\"" + user + "\",\"password\":\"Secret#123\"}";
        post("http://" + Setup.HOST + ":" + Setup.GATEWAY_PORT + "/api/v1/accounts", creds);
        final HttpResponse<String> login =
                post("http://" + Setup.HOST + ":" + Setup.GATEWAY_PORT + "/api/v1/login", creds);
        this.senderSessionId = extract(login.body(), "sessionId");
        assertThat(senderSessionId).as("a valid Sender session id").isNotBlank();
    }

    @When("I call the delivery admin fleet view directly without any identity")
    public void iCallDeliveryAdminDirectlyWithoutIdentity() {
        this.lastDirectStatus = getDirect(null);
    }

    @When("I call the delivery admin fleet view directly with the Sender identity")
    public void iCallDeliveryAdminDirectlyWithSenderIdentity() {
        this.lastDirectStatus = getDirect(senderSessionId);
    }

    @Then("the direct delivery call is rejected with status {int}")
    public void theDirectCallIsRejectedWith(final int expected) {
        assertThat(lastDirectStatus).isEqualTo(expected);
    }

    private int getDirect(final String sessionId) {
        try {
            final HttpRequest.Builder b = HttpRequest.newBuilder(
                            URI.create("http://" + Setup.HOST + ":" + Setup.DELIVERY_ADMIN_PORT + FLEET_PATH))
                    .timeout(Duration.ofSeconds(10)).GET();
            if (sessionId != null) {
                b.header("X-Session-Id", sessionId);
            }
            return HTTP.send(b.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (final Exception e) {
            throw new IllegalStateException("direct call to the delivery admin failed", e);
        }
    }

    private static HttpResponse<String> post(final String url, final String json) {
        try {
            return HTTP.send(HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build(), HttpResponse.BodyHandlers.ofString());
        } catch (final Exception e) {
            throw new IllegalStateException("request to " + url + " failed", e);
        }
    }

    private static String extract(final String body, final String field) {
        final Matcher m = Pattern.compile("\"" + field + "\"\\s*:\\s*\"([^\"]+)\"").matcher(body);
        return m.find() ? m.group(1) : null;
    }
}
