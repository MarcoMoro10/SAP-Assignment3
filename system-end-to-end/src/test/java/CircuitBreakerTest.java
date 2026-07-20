import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import system.steps.Setup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * QA scenario — handling partial failures. With the account-service reachable the circuit is closed
 * ({@code account_circuit_open == 0}). After disconnecting the account-service from the compose network
 * and pushing more than 50% failing REGISTRATIONS through the gateway, the breaker opens
 * ({@code account_circuit_open == 1}). Registration is the gateway->account hop guarded by the breaker:
 * since STEP 6 the gateway is a pure router and login is forwarded to the session-service, so the
 * account breaker is exercised by registration, not by login. The account-service is reconnected
 * afterwards (and the optional return to 0 is checked) so the system is restored for any other test.
 */
class CircuitBreakerTest {

    private static final String ACCOUNT_CONTAINER = "account-service";
    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static final AtomicInteger SEQ = new AtomicInteger();

    private String network;

    @BeforeEach
    void systemUp() {
        Setup.ensureSystemUp();
        network = Setup.dockerNetworkName();
    }

    @AfterEach
    void reconnectAccountService() {
        try {
            Setup.networkConnect(network, ACCOUNT_CONTAINER);
        } catch (final RuntimeException alreadyConnected) {
            // ignore: it was reconnected during the test
        }
        Setup.awaitAccountBreakerClosed();
    }

    @Test
    void circuitOpensWhenTheAccountServiceIsUnreachable() {
        register();
        assertThat(Setup.gatewayMetric("account_circuit_open"))
                .as("circuit must be closed while the account-service is up").isEqualTo(0.0);

        Setup.networkDisconnect(network, ACCOUNT_CONTAINER);

        for (int i = 0; i < 15; i++) {
            register();
        }

        final boolean opened = pollUntil(
                () -> Setup.gatewayMetric("account_circuit_open") == 1.0, Duration.ofSeconds(90));
        assertThat(opened).as("the circuit must open after >50%% of registrations fail").isTrue();

        Setup.networkConnect(network, ACCOUNT_CONTAINER);
        final boolean reclosed = pollUntil(() -> {
            register();
            return Setup.gatewayMetric("account_circuit_open") == 0.0;
        }, Duration.ofSeconds(60));
        assertThat(reclosed).as("the circuit should re-close once the account-service is reachable again").isTrue();
    }

    /** Registration is the gateway->account hop guarded by the breaker (a fresh username each time). */
    private void register() {
        final String user = "cb-" + System.currentTimeMillis() + "-" + SEQ.incrementAndGet();
        final String body = "{\"username\":\"" + user + "\",\"password\":\"Secret#123\"}";
        try {
            HTTP.send(HttpRequest.newBuilder(
                            URI.create("http://" + Setup.HOST + ":" + Setup.GATEWAY_PORT + "/api/v1/accounts"))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build(), HttpResponse.BodyHandlers.discarding());
        } catch (final Exception failFastOrTimeout) {
            // Expected while the account-service is unreachable; the breaker records the failure.
        }
    }

    private static boolean pollUntil(final java.util.function.BooleanSupplier condition, final Duration timeout) {
        final long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.getAsBoolean()) {
                    return true;
                }
            } catch (final RuntimeException metricNotReadyYet) {
                // ignore and retry
            }
            try {
                Thread.sleep(2_000);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }
}
