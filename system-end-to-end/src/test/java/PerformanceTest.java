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
 * QA scenario — THROUGHPUT (SLI-2). Manda un carico di richieste di DOMINIO (login) al gateway, ognuna
 * su un proprio virtual thread, e misura il throughput come DELTA di {@code rest_requests} diviso la
 * finestra temporale del carico. Deve superare i 40 req/s (SLO-2). Le probe di health sono escluse da
 * {@code rest_requests}, quindi il carico deve colpire un endpoint di dominio (non /health/live).
 */
class PerformanceTest {

    private static final int REQUESTS = 2000;
    private static final int WARMUP = 500;
    private static final double MIN_THROUGHPUT_REQ_PER_SEC = 40.0;
    private static final int MAX_IN_FLIGHT = 64;

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

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
    void throughputStaysAboveTheServiceLevelObjective() throws Exception {
        runLoad(WARMUP);

        final double requestsBefore = Setup.gatewayMetric("rest_requests");
        final long startNanos = System.nanoTime();

        final int ok = runLoad(REQUESTS);

        final long elapsedNanos = System.nanoTime() - startNanos;
        final double requestsDelta = Setup.gatewayMetric("rest_requests") - requestsBefore;
        final double elapsedSeconds = elapsedNanos / 1_000_000_000.0;
        final double throughput = requestsDelta / elapsedSeconds;

        System.out.printf("[e2e] throughput: %.1f req/s (%d domain requests in %.2fs, gateway counted %.0f)%n",
                throughput, REQUESTS, elapsedSeconds, requestsDelta);

        assertThat(ok).as("all %d domain requests should complete", REQUESTS).isEqualTo(REQUESTS);
        assertThat(requestsDelta)
                .as("the gateway must have counted at least the %d issued domain requests", REQUESTS)
                .isGreaterThanOrEqualTo(REQUESTS);
        assertThat(throughput)
                .as("throughput must stay above %.0f req/s (SLO-2)", MIN_THROUGHPUT_REQ_PER_SEC)
                .isGreaterThanOrEqualTo(MIN_THROUGHPUT_REQ_PER_SEC);
    }

    private static int runLoad(final int count) throws Exception {
        final AtomicInteger ok = new AtomicInteger(0);
        final Semaphore inFlight = new Semaphore(MAX_IN_FLIGHT);
        try (ExecutorService vthreads = Executors.newVirtualThreadPerTaskExecutor()) {
            final List<Future<?>> futures = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                futures.add(vthreads.submit(() -> {
                    inFlight.acquire();
                    try {
                        if (sendOnce()) {
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

    private static boolean sendOnce() throws Exception {
        try {
            return HTTP.send(loginRequest(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (final java.io.IOException transientError) {
            Thread.sleep(50);
            return HTTP.send(loginRequest(), HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        }
    }
}
