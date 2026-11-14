package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.reactive.messaging.connector.OutboundConnector;

public class OutboxFallbackModeTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    Sender.class, SlowNackConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.fallback-channel.connector", "slow-nack")
            .overrideConfigKey("mp.messaging.outgoing.fallback-channel.outbox.mode", "fallback")
            .overrideConfigKey("mp.messaging.outgoing.fallback-channel.outbox.fallback.timeout", "500ms")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @Inject
    @Any
    SlowNackConnector connector;

    @BeforeEach
    void clear() {
        connector.setMode(SlowNackConnector.Mode.ACK);
    }

    @Test
    void fallbackDefaultConfig_noExplicitRequireTransaction() {
        sender.send("default-config");

        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertThat(connector.received()).contains("default-config"));
    }

    @Test
    void fallbackNackAndTimeoutRace_sendCompletes() {
        connector.setMode(SlowNackConnector.Mode.SLOW_NACK);
        sender.send("race-msg");

        // sendAndAwait completed without error — the nack and timeout shared one store write.
        // The connector sees the message twice (direct attempt + relay delivery), which is expected.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(
                () -> assertThat(connector.delivered()).contains("race-msg"));
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("fallback-channel")
        OutboxEmitter<String> emitter;

        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }
    }

    @ApplicationScoped
    @Connector("slow-nack")
    public static class SlowNackConnector implements OutboundConnector {

        enum Mode {
            ACK,
            SLOW_NACK
        }

        private volatile Mode mode = Mode.ACK;
        private final List<String> received = new CopyOnWriteArrayList<>();
        private final List<String> delivered = new CopyOnWriteArrayList<>();
        private final AtomicReference<Flow.Subscription> subscription = new AtomicReference<>();

        public void setMode(Mode mode) {
            this.mode = mode;
            received.clear();
            delivered.clear();
        }

        public List<String> received() {
            return received;
        }

        public List<String> delivered() {
            return delivered;
        }

        @Override
        public Flow.Subscriber<? extends Message<?>> getSubscriber(Config config) {
            return new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription s) {
                    subscription.set(s);
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Message<?> message) {
                    String payload = String.valueOf(message.getPayload());
                    delivered.add(payload);
                    if (mode == Mode.SLOW_NACK) {
                        CompletableFuture.runAsync(() -> {
                            try {
                                Thread.sleep(400);
                            } catch (InterruptedException ignored) {
                            }
                            message.nack(new RuntimeException("slow nack"));
                        });
                    } else {
                        received.add(payload);
                        message.ack();
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                }

                @Override
                public void onComplete() {
                }
            };
        }
    }
}
