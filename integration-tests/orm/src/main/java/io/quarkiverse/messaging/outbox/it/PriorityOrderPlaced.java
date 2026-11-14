package io.quarkiverse.messaging.outbox.it;

/**
 * Sent through an OutboxEmitter&lt;OrderPlaced&gt;. The stored type is this subclass, so the build must register
 * known subclasses of the emitter's payload type as well.
 */
public class PriorityOrderPlaced extends OrderPlaced {

    public Boolean priority;

    public PriorityOrderPlaced() {
    }

    public PriorityOrderPlaced(String orderId) {
        super(orderId);
        this.priority = Boolean.TRUE;
    }
}
