package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxRelayDisabledTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class, SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.relay-off-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.relay-off-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @Inject
    @Any
    CollectorConnector collector;

    @Test
    void e7_recordsStayInTableWhenRelayDisabled() {
        sender.send("stored-only");

        // Nothing should reach the connector
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
        assertThat(collector.received()).isEmpty();

        // But the record should be in the database
        List<OutboxRecord> records = sender.claimFromStore();
        assertThat(records).hasSize(1);
        assertThat(new String(records.get(0).getPayload())).contains("stored-only");
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("relay-off-channel")
        OutboxEmitter<String> emitter;

        @Inject
        org.hibernate.SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Transactional
        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }

        public List<OutboxRecord> claimFromStore() {
            OutboxStoreContext ctx = new OutboxStoreContext("relay-off-channel", null, "check");
            OutboxStore store = new HibernateOrmOutboxStore(sessionFactory, txRegistry, ctx,
                    java.time.Clock.systemUTC(), false);
            return store.claim(new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                    .await().indefinitely();
        }
    }
}
