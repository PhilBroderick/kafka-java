package io.confluent.developer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

public class ConfluentConfigBuilder {

    public static Properties buildProducer() throws IOException {
        return createProperties(new HashMap<>(){{
            // Event serialization config
            put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
            put(VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");
        }});
    }

    public static Properties buildConsumer(HashMap<String, String> propertyOverrides) throws IOException {
        return createProperties(new HashMap<>(){{
            // Event deserialization config
            put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
            put(VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer");

            putAll(propertyOverrides);
        }});
    }

    private static Properties createProperties(HashMap<String, String> propertyOverrides) throws IOException {
        String propertyFilePath = "src/main/resources/client.properties";
        if (!Files.exists(Paths.get(propertyFilePath))) {
            throw new IOException(propertyFilePath + " not found");
        }

        // Initial set of static properties
        final Properties props = new Properties() {{
            // This controls the durability of messages written to Kafka
            // all - default, guarantees message written to leader + all replicas
            // 0 - maximises throughput, but no delivery guarantee
            // 1 - explicit ack from partition leader of successful write
            put(ACKS_CONFIG, "all");

            // configure authentication and security
            put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            // this configures auth with username + password (API Key)
            put(SASL_MECHANISM, "PLAIN");

            putAll(propertyOverrides);
        }};

        try (FileInputStream fis = new FileInputStream(propertyFilePath)) {
            props.load(fis);
        }

        return props;
    }
}