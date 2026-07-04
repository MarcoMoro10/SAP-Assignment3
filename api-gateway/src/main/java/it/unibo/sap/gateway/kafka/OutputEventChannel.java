package it.unibo.sap.gateway.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

public final class OutputEventChannel {

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public OutputEventChannel(final String topic, final String bootstrapServers) {
        this.topic = topic;
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        this.producer = new KafkaProducer<>(props);
    }

    public void postEvent(final JsonObject event) {
        final JsonObject payload = event.copy().put("timestamp", Instant.now().toString());
        producer.send(new ProducerRecord<>(topic, payload.encode()));
    }

    public String name() {
        return topic;
    }

    public void close() {
        producer.close();
    }
}
