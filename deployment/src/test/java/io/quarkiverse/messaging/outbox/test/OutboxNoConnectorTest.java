package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.runtime.OutboxEmitterFactory;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxNoConnectorTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, Receiver.class));

    @Inject
    Sender sender;

    @Inject
    Receiver receiver;

    @Inject
    @Any
    OutboxEmitterFactory factory;

    @Test
    void applicationStartsWithNoConnector() {
        sender.send("hello");
        await().untilAsserted(() -> assertThat(receiver.lastReceived()).isEqualTo("hello"));
    }

    @Test
    void configIsNullWithNoConnector() {
        assertThat(factory.getChannelConfig("events")).isNull();
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("events")
        OutboxEmitter<String> emitter;

        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }
    }

    @ApplicationScoped
    public static class Receiver {

        private volatile String lastReceived;

        @Incoming("events")
        public void receive(String payload) {
            this.lastReceived = payload;
        }

        public String lastReceived() {
            return lastReceived;
        }
    }
}
