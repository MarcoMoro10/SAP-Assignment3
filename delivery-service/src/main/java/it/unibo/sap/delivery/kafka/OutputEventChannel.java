package it.unibo.sap.delivery.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public final class OutputEventChannel {

    private final String name;
    private final KafkaProducer<String, String> producer;

    public OutputEventChannel(final Vertx vertx, final String name, final String address) {
        this.name = name;
        final Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", address);
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");
        this.producer = KafkaProducer.create(vertx, config);
    }

    public Future<Void> postEvent(final JsonObject ev) {
        final JsonObject payload = ev.copy().put("timestamp", Instant.now().toString());
        final KafkaProducerRecord<String, String> record =
                KafkaProducerRecord.create(name, payload.encode());
        final Promise<Void> promise = Promise.promise();
        producer.send(record)
                .onSuccess(metadata -> promise.complete())
                .onFailure(promise::fail);
        return promise.future();
    }

    public String name() {
        return name;
    }

    public void close() {
        producer.close();
    }
}
