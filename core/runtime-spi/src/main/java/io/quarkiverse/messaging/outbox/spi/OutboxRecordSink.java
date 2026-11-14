package io.quarkiverse.messaging.outbox.spi;

import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * The relay side of an {@link OutboxRecordSource}: where the source hands its records.
 * <p>
 * Called on the relay's context only.
 */
public interface OutboxRecordSink {

    /** How many more records can be accepted now. */
    int remainingCapacity();

    /**
     * Under {@code ordering=key}, the keys the relay is working on. While a key is here after the relay handed
     * records of it back with {@link OutboxRecordSource#released}, the source must not hand out records of that
     * key. Empty when the channel is not ordered by key.
     */
    Set<String> busyGroups();

    /** The ids of the records accepted and not settled yet. */
    List<UUID> heldIds();

    /** Takes records to send, in the order they must be sent. Records already held are ignored. */
    void accept(List<OutboxRecord> records);
}
