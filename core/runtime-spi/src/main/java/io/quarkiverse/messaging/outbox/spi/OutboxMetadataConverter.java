package io.quarkiverse.messaging.outbox.spi;

import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.inject.spi.Prioritized;
import jakarta.interceptor.Interceptor;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

/**
 * Extracts connector-specific metadata from an outgoing message on the write side, and restores it on the
 * relay side. Each connector (Kafka, AMQP, RabbitMQ) provides an implementation qualified with
 * {@code @Identifier(connectorName)}. Cross-cutting converters (e.g., tracing) have no qualifier and apply
 * to all channels.
 */
public interface OutboxMetadataConverter extends Prioritized {

    /** Extracts the ordering key from connector metadata (e.g., the Kafka record key). */
    default Optional<String> extractKey(Message<?> message) {
        return Optional.empty();
    }

    /** Extracts the payload value from a wrapper type (e.g., {@code Record<K,V>.value()}). */
    default Optional<Object> extractValue(Message<?> message) {
        return Optional.empty();
    }

    /** Extracts connector-specific metadata from the message and returns it as a map to store in the outbox headers. */
    Map<String, Object> extract(Message<?> message);

    /** Restores connector-specific metadata from the outbox record into {@link Metadata} for the outgoing message. */
    Metadata restore(OutboxRecord record);

    @Override
    default int getPriority() {
        return DEFAULT_PRIORITY;
    }

    int DEFAULT_PRIORITY = Interceptor.Priority.APPLICATION;
}
