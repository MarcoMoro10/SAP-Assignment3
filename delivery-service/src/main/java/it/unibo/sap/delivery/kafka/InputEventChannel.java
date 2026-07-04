package it.unibo.sap.delivery.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;


public final class InputEventChannel {

    private final String topic;
    private final KafkaConsumer<String, String> consumer;
    private volatile boolean running = true;

    public InputEventChannel(final String topic, final String bootstrapServers) {
        this.topic = topic;
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topic + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void init(final Consumer<JsonObject> handler) {
        consumer.subscribe(List.of(topic));
        final Thread poller = new Thread(() -> {
            try {
                while (running) {
                    final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (final ConsumerRecord<String, String> record : records) {
                        handler.accept(new JsonObject(record.value()));
                    }
                }
            } finally {
                consumer.close();
            }
        }, "input-event-channel-" + topic);
        poller.setDaemon(true);
        poller.start();
    }

    public String name() {
        return topic;
    }

    public void stop() {
        running = false;
    }
}
