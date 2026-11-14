package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxKeyTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.customer-events.connector", "test-collector");

    @Inject
    Sender sender;

    @Inject
    @Any
    CollectorConnector collector;

    @Test
    void sendWithKeyAttachesOutboxMetadata() {
        sender.send("k1", "payload");
        await().untilAsserted(() -> assertThat(collector.received()).hasSize(1));

        Message<?> received = collector.received().get(0);
        Optional<OutboxMetadata> metadata = received.getMetadata(OutboxMetadata.class);
        assertThat(metadata).isPresent();
        assertThat(metadata.get().getKey()).hasValue("k1");
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("customer-events")
        OutboxEmitter<String> emitter;

        public void send(String key, String payload) {
            emitter.sendAndAwait(key, payload);
        }
    }
}
