## Kafka in Java

This is an example repository scaffolded from the [Java Getting Started](https://developer.confluent.io/get-started/java/) tutorial
on the Confluent docs.

There is a very simple Producer class that creates purchase events and publishes them to a `purchases` topic.

A consumer class then subscribes to the `purchases` topic and streams the output to the console.


### Pre-requisites

The producer/consumer classes are currently configured to connect to a local Kafka broker, which can be spun up with:
`confluent local kafka start`

A topic is then required to publish events to: `confluent local kafka topic create purchases`