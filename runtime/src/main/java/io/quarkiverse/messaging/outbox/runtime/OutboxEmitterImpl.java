package io.quarkiverse.messaging.outbox.runtime;

import java.util.concurrent.Flow.Publisher;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;
import io.smallrye.reactive.messaging.MessagePublisherProvider;

public class OutboxEmitterImpl<T> implements OutboxEmitter<T>, MessagePublisherProvider<Object> {

    private final Multi<Message<? extends Object>> publisher;
    private volatile MultiEmitter<? super Message<? extends Object>> internalEmitter;
    private final String channel;

    public OutboxEmitterImpl(String channel, long defaultBufferSize) {
        this.channel = channel;
        this.publisher = Multi.createFrom().<Message<? extends Object>> emitter(
                emitter -> this.internalEmitter = emitter);
    }

    @Override
    public Uni<Void> send(T payload) {
        return sendMessage(Message.of(payload));
    }

    @Override
    public Uni<Void> send(String key, T payload) {
        Message<T> message = Message.of(payload, Metadata.of(OutboxMetadata.withKey(key)));
        return sendMessage(message);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <M extends Message<? extends T>> Uni<Void> sendMessage(M message) {
        return Uni.createFrom().emitter(uniEmitter -> {
            Message<Object> enriched = ((Message<Object>) message)
                    .withAck(() -> {
                        uniEmitter.complete(null);
                        return message.ack();
                    })
                    .withNack(t -> {
                        uniEmitter.fail(t);
                        return message.nack(t);
                    });
            if (internalEmitter == null) {
                uniEmitter.fail(new IllegalStateException("No subscriber for channel " + channel));
                return;
            }
            internalEmitter.emit(enriched);
        });
    }

    @Override
    public void sendAndAwait(T payload) {
        send(payload).await().indefinitely();
    }

    @Override
    public void sendAndAwait(String key, T payload) {
        send(key, payload).await().indefinitely();
    }

    @Override
    public <M extends Message<? extends T>> void sendMessageAndAwait(M message) {
        sendMessage(message).await().indefinitely();
    }

    @Override
    public Publisher<Message<? extends Object>> getPublisher() {
        return publisher;
    }

    public String getChannel() {
        return channel;
    }
}
