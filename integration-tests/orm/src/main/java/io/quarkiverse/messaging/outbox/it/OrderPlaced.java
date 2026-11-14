package io.quarkiverse.messaging.outbox.it;

/** A plain object payload. In a native image it is only deserializable if the build registers it. */
public class OrderPlaced {

    public String orderId;

    public OrderPlaced() {
    }

    public OrderPlaced(String orderId) {
        this.orderId = orderId;
    }
}
