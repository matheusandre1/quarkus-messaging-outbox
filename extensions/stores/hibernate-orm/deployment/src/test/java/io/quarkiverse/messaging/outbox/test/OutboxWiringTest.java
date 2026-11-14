package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxWiringTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.customer-events.connector", "test-collector")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @Inject
    @Any
    CollectorConnector collector;

    @Test
    void messageSentThroughOutboxEmitterReachesTheSink() {
        sender.send("hello");
        await().untilAsserted(() -> assertThat(collector.received()).hasSize(1));
        assertThat(collector.received().get(0).getPayload()).isEqualTo("hello");
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("customer-events")
        OutboxEmitter<String> emitter;

        @Transactional
        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }
    }
}
