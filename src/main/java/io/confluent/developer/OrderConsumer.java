package io.confluent.developer;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

public class OrderConsumer {

    public static void main(final String[] args) throws IOException {

        final Properties properties = ConfluentConfigBuilder.buildConsumer(new HashMap<>() {{
            // group id is required for consumers to ensure multiple instances of the same application/consumer
            // can process messages from the same topic in parallel
            put(GROUP_ID_CONFIG, "order-consumer-01");

            // ensures consumers in the group start reading from the earliest message (start of the log)
            // This is best for data integrity as no messages will be missed
            // However, this can be a performance hit for very large topics
            put(AUTO_OFFSET_RESET_CONFIG, "earliest");

            // Ensure the specific Order class is returned in the ConsumerRecord class
            // Without this, it will return a generic LinkedHashMap (for JSON)
            put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Order.class.getName());
        }});

        final String topic = "orders";

        consumeMessages(topic, properties);
    }

    private static void consumeMessages(final String topic, final Properties properties) {
        try (final KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (true) {
                System.out.println("Consuming messages...");

                ConsumerRecords<String, Order> orders = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Order> record : orders) {
                    final String key = record.key();
                    final Order order = record.value();
                    System.out.println("Key: " + key + ", Order: " + order);
                    System.out.println(
                            "Order id: " + order.orderId + ", ordered on: " + order.orderTime
                    );
                }
            }
        }
    }
}
