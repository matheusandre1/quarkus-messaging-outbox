package io.quarkiverse.messaging.outbox.spi;

import java.lang.reflect.Type;

import org.eclipse.microprofile.reactive.messaging.Message;

/**
 * Serializes messages to outbox records and deserializes them back. The default implementation uses Jackson.
 * Applications can provide their own bean to replace it, for example for Avro payloads.
 */
public interface OutboxRecordMapper {

    /** Serializes the message payload and builds an {@link OutboxRecord} for the given channel. */
    OutboxRecord toRecord(String channel, Message<?> message);

    /** Deserializes an {@link OutboxRecord} back into a {@link Message}, using {@code payloadType} for the payload. */
    Message<?> toMessage(OutboxRecord record, Type payloadType);
}
