import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import system.steps.Setup;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * QA scenario — AVAILABILITY (SLI-1). Availability = successful_rest_requests / rest_requests (SLO-1: > 99%).
 * Lancia un batch di richieste di DOMINIO che hanno SUCCESSO end-to-end (login valido -> 200) e verifica
 * il rapporto sul DELTA prodotto da questo carico (i contatori del gateway sono cumulativi dall'avvio, e
 * altri test possono aver introdotto 4xx: si misura la finestra). Le probe di health sono escluse dagli
 * SLI, quindi non falsano il rapporto.
 */
class AvailabilityTest {

    private static final int REQUESTS = 300;
    private static final int MAX_IN_FLIGHT = 64;
    private static final double MIN_AVAILABILITY = 0.99;

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static HttpRequest loginRequest() {
        final String body = "{\"username\":\"" + Setup.ADMIN_USERNAME
                + "\",\"password\":\"" + Setup.ADMIN_PASSWORD + "\"}";
        return HttpRequest.newBuilder(
                        URI.create("http://" + Setup.HOST + ":" + Setup.GATEWAY_PORT + "/api/v1/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    @BeforeAll
    static void systemUp() {
        Setup.ensureSystemUp();
        Setup.awaitAccountBreakerClosed();
    }

    @Test
    void availabilityStaysAboveTheServiceLevelObjective() throws Exception {
        final double successBefore = Setup.gatewayMetric("successful_rest_requests");
        final double requestsBefore = Setup.gatewayMetric("rest_requests");

        final int ok = runSuccessfulLoad(REQUESTS);
        assertThat(ok).as("all %d domain requests should succeed end-to-end", REQUESTS).isEqualTo(REQUESTS);

        final double successDelta = Setup.gatewayMetric("successful_rest_requests") - successBefore;
        final double requestsDelta = Setup.gatewayMetric("rest_requests") - requestsBefore;
        final double availability = successDelta / requestsDelta;

        System.out.printf("[e2e] availability: %.4f (%.0f successful / %.0f total in the window)%n",
                availability, successDelta, requestsDelta);

        assertThat(requestsDelta)
                .as("the gateway must have counted the issued domain requests")
                .isGreaterThanOrEqualTo(REQUESTS);
        assertThat(availability)
                .as("availability must stay above %.0f%% (SLO-1)", MIN_AVAILABILITY * 100)
                .isGreaterThan(MIN_AVAILABILITY);
    }

    private static int runSuccessfulLoad(final int count) throws Exception {
        final AtomicInteger ok = new AtomicInteger(0);
        final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        try (ExecutorService vthreads = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(vthreads.submit(() -> {
                    inFlight.acquire();
                    try {
                        if (loginOnce()) {
                            ok.incrementAndGet();
                        }
                    } finally {
                        inFlight.release();
                    }
                    return null;
                }));
            }
            for (final Future<?> f : futures) {
                f.get();
            }
        }
        return ok.get();
    }

    private static boolean loginOnce() throws Exception {
        try {
            return HTTP.send(loginRequest(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (final java.io.IOException transientError) {
            Thread.sleep(50);
            return HTTP.send(loginRequest(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        }
    }
}
