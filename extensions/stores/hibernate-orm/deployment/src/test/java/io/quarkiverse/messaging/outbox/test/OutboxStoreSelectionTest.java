package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.TransactionSynchronizationRegistry;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;

public class OutboxStoreSelectionTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    Sender.class, CollectorConnector.class, DummyStoreFactory.class))
            .overrideConfigKey("mp.messaging.outgoing.store-test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.store-test-channel.outbox.store", "hibernate-orm")
            .overrideConfigKey("mp.messaging.outgoing.store-test-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @BeforeEach
    void cleanTable() {
        sender.deleteAll();
        DummyStoreFactory.createCalled = false;
    }

    @Test
    void explicitStoreSelectionUsesCorrectStore() {
        sender.send("test-msg");

        OutboxStore store = sender.createStore();
        List<OutboxRecord> claimed = store.claim(new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);
        assertThat(new String(claimed.get(0).getPayload())).contains("test-msg");

        assertThat(DummyStoreFactory.createCalled)
                .as("dummy store should not be used when hibernate-orm is selected")
                .isFalse();
    }

    @ApplicationScoped
    @Identifier("dummy")
    public static class DummyStoreFactory implements OutboxStore.Factory {

        static volatile boolean createCalled = false;

        @Override
        public OutboxStore create(OutboxStoreContext context) {
            createCalled = true;
            throw new UnsupportedOperationException("dummy store should not be used");
        }
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        SessionFactory sessionFactory;

        @Inject
        TransactionSynchronizationRegistry txRegistry;

        @Inject
        @Channel("store-test-channel")
        OutboxEmitter<String> emitter;

        @Transactional
        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }

        public OutboxStore createStore() {
            OutboxStoreContext context = new OutboxStoreContext("store-test-channel", null, "test-relay");
            return new HibernateOrmOutboxStore(sessionFactory, txRegistry, context, Clock.systemUTC(), true);
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession().createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
        }
    }
}
