package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxCoexistenceTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(DualSender.class, CollectorConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.outbox-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.standard-channel.connector", "test-collector")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    DualSender sender;

    @Inject
    @Any
    CollectorConnector collector;

    @Test
    void outboxEmitterAndStandardEmitterCoexist() {
        sender.sendViaOutbox("outbox-msg");
        sender.sendViaStandard("standard-msg");

        await().untilAsserted(() -> assertThat(collector.received()).hasSize(2));
        assertThat(collector.received()).extracting(m -> (Object) m.getPayload())
                .containsExactlyInAnyOrder("outbox-msg", "standard-msg");
    }

    @ApplicationScoped
    public static class DualSender {

        @Inject
        @Channel("outbox-channel")
        OutboxEmitter<String> outboxEmitter;

        @Inject
        @Channel("standard-channel")
        Emitter<String> standardEmitter;

        @Transactional
        public void sendViaOutbox(String payload) {
            outboxEmitter.sendAndAwait(payload);
        }

        public void sendViaStandard(String payload) {
            standardEmitter.send(payload);
        }
    }
}
