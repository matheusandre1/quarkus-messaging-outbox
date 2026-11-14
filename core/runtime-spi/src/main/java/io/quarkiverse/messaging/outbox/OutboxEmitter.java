package io.quarkiverse.messaging.outbox;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

/**
 * A drop-in replacement for {@code MutinyEmitter} that stores outgoing messages in the caller's database
 * transaction and relays them to the channel's connector with at-least-once delivery.
 * <p>
 * Inject it on an outgoing channel with {@code @Channel}. The {@code send} methods are lazy: nothing is
 * written until the returned {@link Uni} is subscribed. In blocking code, use the {@code ...AndAwait} variants.
 *
 * @param <T> the payload type
 */
@Experimental("Outbox emitter")
public interface OutboxEmitter<T> extends EmitterType {

    /**
     * Stores the payload in the outbox table. The returned {@link Uni} completes when the record is written,
     * not when the broker acknowledges it.
     */
    Uni<Void> send(T payload);

    /** Stores the payload with an explicit ordering key. */
    Uni<Void> send(String key, T payload);

    /** Stores a full {@link Message}, preserving its metadata. */
    <M extends Message<? extends T>> Uni<Void> sendMessage(M message);

    /** Blocking variant of {@link #send(Object)}. Subscribes the Uni and waits for completion. */
    void sendAndAwait(T payload);

    /** Blocking variant of {@link #send(String, Object)}. */
    void sendAndAwait(String key, T payload);

    /** Blocking variant of {@link #sendMessage(Message)}. */
    <M extends Message<? extends T>> void sendMessageAndAwait(M message);
}
