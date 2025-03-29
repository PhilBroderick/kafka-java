package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.CommonClientConfigs.SECURITY_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SaslConfigs.SASL_MECHANISM;

public class OrderProducer {

    public static void main(final String[] args) throws IOException {

        final Properties properties = readConfig("src/main/resources/client.properties");

        final String topic = "orders";

        produceMessages(topic, properties);
    }

    public static Properties readConfig(final String configFile) throws IOException {

        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found");
        }

        // Initial set of static properties
        final Properties props = new Properties() {{

            // Event serialization config
            put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
            put(VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer");

            // This controls the durability of messages written to Kafka
            // all - default, guarantees message written to leader + all replicas
            // 0 - maximises throughput, but no delivery guarantee
            // 1 - explicit ack from partition leader of successful write
            put(ACKS_CONFIG, "all");

            // configure authentication and security
            put(SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
            // this configures auth with username + password (API Key)
            put(SASL_MECHANISM, "PLAIN");
        }};

        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        }

        return props;
    }

    private static void produceMessages(String topic, Properties properties) throws IOException {
        final String ordersFileLocation = "src/main/resources/orders.v1.txt";

//        if (!Files.exists(Paths.get(ordersFileLocation))) {
//            throw new IOException(ordersFileLocation + " not found");
//        }

        Order[] orders = new Order[10];
        final Random rnd = new Random();
        final Item[] items = {
                new Item(1, "Carbon wheels", new BigDecimal("1099.99")),
                new Item(2, "Wahoo Elemnt Bolt GPS Computer", new BigDecimal("299")),
                new Item(3, "Wax chain kit", new BigDecimal("99.98")),
                new Item(4, "Bar tape", new BigDecimal("29.99")),
                new Item(5, "Aero TT helmet", new BigDecimal("349.99")),
                new Item(6, "Wetsuit", new BigDecimal("799")),
                new Item(7, "Carbon running shoes", new BigDecimal("167.90"))
        };

        for (int i=0; i<10; i++) {
            orders[i] = new Order(i, rnd.nextInt(10), new Item[]{
                    items[rnd.nextInt(items.length)],
            });
        }

        try (final Producer<String, Order> producer = new KafkaProducer<>(properties)) {
            for (Order order : orders) {
                producer.send(
                        new ProducerRecord<>(topic, String.valueOf(order.orderId), order),
                        (metadata, exception) -> {
                            if (exception != null) {
                                exception.printStackTrace();
                            } else {
                                System.out.println("Sent " + order.orderId + " to " + metadata.topic());
                            }
                        }
                );
            }
        }

    }
}

