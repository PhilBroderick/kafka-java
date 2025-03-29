package io.confluent.developer;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

public class Item {

    @JsonProperty
    public int itemId;

    @JsonProperty
    public String name;

    @JsonProperty
    public BigDecimal price;
    
    public Item(int id, String name, BigDecimal price) {
        this.itemId = id;
        this.name = name;
        this.price = price;
    }
}
