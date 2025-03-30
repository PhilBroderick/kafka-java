package io.confluent.developer;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;

public class Order {

    @JsonProperty
    public int orderId;

    @JsonProperty
    public int userId;

    @JsonProperty
    public LocalDateTime orderTime;

    @JsonProperty
    public Item[] items;

    public Order(int id, int userId, Item[] items) {
        this.orderId = id;
        this.userId = userId;
        this.orderTime = LocalDateTime.now();
        this.items = items;
    }

    // Required for deserialization
    public Order() {}
}

