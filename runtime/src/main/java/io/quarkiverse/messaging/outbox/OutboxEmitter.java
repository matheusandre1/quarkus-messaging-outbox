package io.quarkiverse.messaging.outbox;

import org.eclipse.microprofile.reactive.messaging.Message;

import io.smallrye.common.annotation.Experimental;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.EmitterType;

@Experimental("Outbox emitter")
public interface OutboxEmitter<T> extends EmitterType {

    Uni<Void> send(T payload);

    Uni<Void> send(String key, T payload);

    <M extends Message<? extends T>> Uni<Void> sendMessage(M message);

    void sendAndAwait(T payload);

    void sendAndAwait(String key, T payload);

    <M extends Message<? extends T>> void sendMessageAndAwait(M message);
}
