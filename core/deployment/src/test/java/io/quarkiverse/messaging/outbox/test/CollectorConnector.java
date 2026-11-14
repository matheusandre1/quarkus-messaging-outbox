package io.quarkiverse.messaging.outbox.test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;

import io.smallrye.reactive.messaging.connector.OutboundConnector;

@ApplicationScoped
@Connector("test-collector")
public class CollectorConnector implements OutboundConnector {

    private final List<Message<?>> received = new CopyOnWriteArrayList<>();

    @Override
    public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
        return new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Message<?> message) {
                received.add(message);
                message.ack();
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
        };
    }

    public List<Message<?>> received() {
        return received;
    }

    public void clear() {
        received.clear();
    }
}
