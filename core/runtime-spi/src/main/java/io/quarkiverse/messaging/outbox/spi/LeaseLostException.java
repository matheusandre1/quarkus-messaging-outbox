package io.quarkiverse.messaging.outbox.spi;

/**
 * Thrown by store operations when the channel lease has moved to another relay. The relay stops
 * and drops its in-memory state; the new lease holder picks the records up.
 */
public class LeaseLostException extends RuntimeException {

    public LeaseLostException(String message) {
        super(message);
    }
}
