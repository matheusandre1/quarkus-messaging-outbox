package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

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

public class OutboxCustomTableNameTest {

    static final String CUSTOM_TABLE = "my_custom_outbox";
    static final String CUSTOM_LEASE_TABLE = "my_custom_lease";

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    TestService.class, CollectorConnector.class, SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.test-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.messaging-outbox.table-name", CUSTOM_TABLE)
            .overrideConfigKey("quarkus.messaging-outbox.lease-table-name", CUSTOM_LEASE_TABLE);

    @Inject
    TestService service;

    @BeforeEach
    void cleanTable() {
        service.deleteAll();
    }

    @Test
    void customTableNameIsUsed() {
        OutboxStore store = service.createStore();

        AtomicBoolean signaled = new AtomicBoolean();
        service.persistInTransaction(store, "hello-custom", () -> signaled.set(true));
        assertThat(signaled.get()).isTrue();

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);
        assertThat(new String(claimed.get(0).getPayload())).isEqualTo("hello-custom");

        Long count = service.countInCustomTable();
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void leaseCustomTableNameIsUsed() {
        OutboxStore store = service.createStore();
        boolean acquired = store.holdLease(Duration.ofSeconds(10)).await().indefinitely();
        assertThat(acquired).isTrue();

        Long count = service.countInCustomLeaseTable();
        assertThat(count).isEqualTo(1L);
    }

    @ApplicationScoped
    public static class TestService {

        @Inject
        @org.eclipse.microprofile.reactive.messaging.Channel("test-channel")
        OutboxEmitter<String> emitter;

        @Inject
        SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        public OutboxStore createStore() {
            return new HibernateOrmOutboxStore(
                    sessionFactory, txRegistry,
                    new OutboxStoreContext("test-channel", null, UUID.randomUUID().toString()),
                    Clock.systemUTC(), true);
        }

        @Transactional
        public void persistInTransaction(OutboxStore store, String payload, Runnable afterCommit) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel("test-channel");
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            record.setStatus(OutboxRecord.STATUS_PENDING);
            store.persist(record, afterCommit).await().indefinitely();
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxLeaseEntity").executeUpdate();
        }

        public Long countInCustomTable() {
            try (var session = sessionFactory.openStatelessSession()) {
                return session.createNativeQuery(
                        "SELECT count(*) FROM " + CUSTOM_TABLE, Long.class).getSingleResult();
            }
        }

        public Long countInCustomLeaseTable() {
            try (var session = sessionFactory.openStatelessSession()) {
                return session.createNativeQuery(
                        "SELECT count(*) FROM " + CUSTOM_LEASE_TABLE, Long.class).getSingleResult();
            }
        }
    }
}
