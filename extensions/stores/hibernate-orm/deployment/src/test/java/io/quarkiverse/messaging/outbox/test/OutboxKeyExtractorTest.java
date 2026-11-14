package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.List;
import java.util.Set;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
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
import io.smallrye.reactive.messaging.keyed.KeyValueExtractor;

public class OutboxKeyExtractorTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    Sender.class, CollectorConnector.class, SimpleStringMapper.class,
                    PrefixKeyExtractor.class))
            .overrideConfigKey("mp.messaging.outgoing.extract-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.extract-channel.outbox.key-extractor", "PrefixKeyExtractor")
            .overrideConfigKey("mp.messaging.outgoing.extract-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    Sender sender;

    @BeforeEach
    void cleanTable() {
        sender.deleteAll();
    }

    @Test
    void e13_namedKeyExtractorIsUsed() {
        sender.send("order-123");

        List<OutboxRecord> records = sender.claimFromStore();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getMsgKey()).isEqualTo("key:order-123");
    }

    @Test
    void e13_explicitKeySupersedesExtractor() {
        sender.sendWithKey("explicit-key", "order-456");

        List<OutboxRecord> records = sender.claimFromStore();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).getMsgKey()).isEqualTo("explicit-key");
    }

    @ApplicationScoped
    public static class PrefixKeyExtractor implements KeyValueExtractor {

        @Override
        public boolean canExtract(Message<?> first, Type keyType, Type valueType) {
            return true;
        }

        @Override
        public Object extractKey(Message<?> message, Type keyType) {
            return "key:" + message.getPayload();
        }

        @Override
        public Object extractValue(Message<?> message, Type valueType) {
            return message.getPayload();
        }
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("extract-channel")
        OutboxEmitter<String> emitter;

        @Inject
        org.hibernate.SessionFactory sessionFactory;

        @Inject
        jakarta.transaction.TransactionSynchronizationRegistry txRegistry;

        @Transactional
        public void send(String payload) {
            emitter.sendAndAwait(payload);
        }

        @Transactional
        public void sendWithKey(String key, String payload) {
            emitter.sendAndAwait(key, payload);
        }

        public List<OutboxRecord> claimFromStore() {
            OutboxStoreContext ctx = new OutboxStoreContext("extract-channel", null, "check");
            OutboxStore store = new HibernateOrmOutboxStore(sessionFactory, txRegistry, ctx,
                    java.time.Clock.systemUTC(), false);
            return store.claim(new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                    .await().indefinitely();
        }

        @Transactional
        public void deleteAll() {
            sessionFactory.getCurrentSession()
                    .createMutationQuery("delete from OutboxRecordEntity").executeUpdate();
        }
    }
}
