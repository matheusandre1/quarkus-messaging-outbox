package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStore;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.spi.OutboxStoreContext;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxCustomMapperTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    TestService.class, CollectorConnector.class, PrefixMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.custom-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.custom-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    TestService service;

    @BeforeEach
    void cleanTable() {
        service.deleteAll();
    }

    @Test
    void e8_customMapperIsUsedForPersist() {
        OutboxStore store = service.createStore();
        service.persistViaMapper(store, "hello");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).hasSize(1);
        assertThat(new String(claimed.get(0).getPayload())).isEqualTo("CUSTOM:hello");
    }

    @ApplicationScoped
    public static class PrefixMapper implements OutboxRecordMapper {

        @Override
        public OutboxRecord toRecord(String channel, Message<?> message) {
            OutboxRecord record = new OutboxRecord();
            record.setId(UUID.randomUUID());
            record.setChannel(channel);
            String payload = "CUSTOM:" + message.getPayload();
            record.setPayload(payload.getBytes());
            record.setPayloadType("java.lang.String");
            message.getMetadata(OutboxMetadata.class)
                    .ifPresent(meta -> meta.getKey().ifPresent(record::setMsgKey));
            return record;
        }

        @Override
        public Message<?> toMessage(OutboxRecord record, Type payloadType) {
            return Message.of(new String(record.getPayload()),
                    Metadata.of(OutboxMetadata.withKey(record.getMsgKey())));
        }
    }

    @ApplicationScoped
    public static class TestService {

        @Inject
        org.hibernate.SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Inject
        OutboxRecordMapper mapper;

        @Inject
        @Channel("custom-channel")
        OutboxEmitter<String> emitter;

        public OutboxStore createStore() {
            OutboxStoreContext context = new OutboxStoreContext("custom-channel", null, "test-relay");
            return new HibernateOrmOutboxStore(sessionFactory, txRegistry, context,
                    java.time.Clock.systemUTC(), true);
        }

        @Transactional
        public void persistViaMapper(OutboxStore store, String payload) {
            OutboxRecord record = mapper.toRecord("custom-channel", Message.of(payload));
            store.persist(record, () -> {
            }).await().indefinitely();
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
        }
    }
}
