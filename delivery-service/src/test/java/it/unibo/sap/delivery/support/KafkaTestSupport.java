package it.unibo.sap.delivery.support;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assumptions;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Helper per i test di integrazione che richiedono un broker Kafka reale.
 */
public final class KafkaTestSupport {

    private KafkaTestSupport() {
    }

    public static String brokerAddress() {
        final String v = System.getenv("EV_CHANNELS_LOCATION");
        return (v == null || v.isBlank()) ? "localhost:29092" : v;
    }

    public static void assumeBrokerReachable() {
        final String addr = brokerAddress();
        final String[] hp = addr.split(":");
        final String host = hp[0];
        final int port = hp.length > 1 ? Integer.parseInt(hp[1].trim()) : 9092;
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), 1500);
        } catch (final Exception unreachable) {
            Assumptions.abort("Kafka broker not reachable at " + addr + " — skipping Kafka integration test");
        }
    }

    public static KafkaConsumer<String, String> consumer(final Vertx vertx, final String topic,
                                                         final Consumer<JsonObject> handler) {
        final Map<String, String> cfg = new HashMap<>();
        cfg.put("bootstrap.servers", brokerAddress());
        cfg.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.put("group.id", "test-" + UUID.randomUUID());
        cfg.put("auto.offset.reset", "earliest");
        cfg.put("enable.auto.commit", "true");
        final KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, cfg);
        consumer.handler(rec -> handler.accept(new JsonObject(rec.value())));
        consumer.subscribe(topic);
        return consumer;
    }
}
