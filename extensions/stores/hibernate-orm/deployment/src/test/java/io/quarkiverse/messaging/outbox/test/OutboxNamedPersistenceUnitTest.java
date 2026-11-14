package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.hibernate.orm.PersistenceUnit;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxNamedPersistenceUnitTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(TestService.class, CollectorConnector.class,
                    SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.named-pu-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.named-pu-channel.outbox.persistence-unit", "outbox")
            .overrideConfigKey("mp.messaging.outgoing.named-pu-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.hibernate-orm.datasource", "<default>")
            .overrideConfigKey("quarkus.hibernate-orm.packages", "io.quarkiverse.messaging.outbox.store.persistence")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.hibernate-orm.\"outbox\".schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.hibernate-orm.\"outbox\".datasource", "outbox")
            .overrideConfigKey("quarkus.hibernate-orm.\"outbox\".packages",
                    "io.quarkiverse.messaging.outbox.store.persistence")
            .overrideConfigKey("quarkus.datasource.\"outbox\".db-kind", "postgresql");

    @Inject
    TestService service;

    @BeforeEach
    void cleanTable() {
        service.deleteAll();
    }

    @Test
    void e6_namedPersistenceUnitStoresRecords() {
        service.persistInTransaction("named-pu-msg");

        List<OutboxRecord> records = service.claimFromNamedPu();
        assertThat(records).hasSize(1);
        assertThat(new String(records.get(0).getPayload())).contains("named-pu-msg");
    }

    @ApplicationScoped
    public static class TestService {

        @Inject
        @PersistenceUnit("outbox")
        org.hibernate.SessionFactory namedSessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Inject
        @Channel("named-pu-channel")
        OutboxEmitter<String> emitter;

        @Transactional
        public void persistInTransaction(String payload) {
            emitter.sendAndAwait(payload);
        }

        public List<OutboxRecord> claimFromNamedPu() {
            OutboxStoreContext ctx = new OutboxStoreContext("named-pu-channel", null, "check");
            OutboxStore store = new HibernateOrmOutboxStore(namedSessionFactory, txRegistry, ctx,
                    java.time.Clock.systemUTC(), false);
            return store.claim(new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                    .await().indefinitely();
        }

        @Transactional
        public void deleteAll() {
            namedSessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
        }
    }
}
