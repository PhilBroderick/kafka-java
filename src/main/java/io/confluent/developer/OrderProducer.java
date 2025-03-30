package io.confluent.developer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;

public class OrderProducer {

    public static void main(final String[] args) throws IOException {

        final Properties properties = ConfluentConfigBuilder.buildProducer();

        final String topic = "orders";

        produceMessages(topic, properties);
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

