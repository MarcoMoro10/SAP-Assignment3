package it.unibo.sap.gateway.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;


public final class InputEventChannel {

    private static final String GROUP_ID = "api-gateway";
    private static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";

    private final String name;
    private final KafkaConsumer<String, String> consumer;


    public InputEventChannel(final Vertx vertx, final String name, final String address) {
        this(vertx, name, address, GROUP_ID, DEFAULT_AUTO_OFFSET_RESET);
    }

    public InputEventChannel(final Vertx vertx, final String name, final String address,
                             final String groupId, final String autoOffsetReset) {
        this.name = name;
        final Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", address);
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", groupId);
        config.put("auto.offset.reset", autoOffsetReset);
        config.put("enable.auto.commit", "true");
        this.consumer = KafkaConsumer.create(vertx, config);
    }


    public Future<Void> init(final Consumer<JsonObject> handler) {
        consumer.handler(record -> handler.accept(new JsonObject(record.value())));
        final Promise<Void> promise = Promise.promise();
        consumer.subscribe(name)
                .onSuccess(v -> promise.complete())
                .onFailure(promise::fail);
        return promise.future();
    }

    public String name() {
        return name;
    }

    public void close() {
        consumer.close();
    }
}
