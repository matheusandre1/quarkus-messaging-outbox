package io.quarkiverse.messaging.outbox.debezium.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.spi.Connector;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.reactive.messaging.memory.TestingConnector;

/**
 * The Debezium record source against PostgreSQL with logical replication, through the Hibernate ORM store.
 */
public class DebeziumRecordSourceTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Writer.class))
            .overrideConfigKey("mp.messaging.outgoing.cdc-events.connector", "smallrye-testing")
            .overrideConfigKey("mp.messaging.outgoing.cdc-events.outbox.relay.source", "debezium")
            .overrideConfigKey("mp.messaging.outgoing.cdc-events.outbox.ordering", "strict")
            .overrideConfigKey("mp.messaging.outgoing.cdc-events.outbox.lease-duration", "3s")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .overrideConfigKey("quarkus.datasource.devservices.command", "postgres -c wal_level=logical");

    @Inject
    Writer writer;

    @Inject
    @Connector("smallrye-testing")
    TestingConnector connector;

    @BeforeEach
    void clear() {
        connector.outgoing("cdc-events").clear();
    }

    private List<String> received() {
        return connector.outgoing("cdc-events").sent().stream().map(Message::getPayload).map(String::valueOf).toList();
    }

    @Test
    void recordsAreRelayedInOrderAndRemovedOnceDelivered() {
        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            writer.write("in-order-" + i);
            sent.add("in-order-" + i);
        }

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(received()).containsAll(sent));
        assertThat(received().stream().filter(p -> p.startsWith("in-order-")).toList()).containsExactlyElementsOf(sent);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(writer.pending()).isZero());
    }

    /**
     * The polling source sends records in insert order. Here the first transaction to insert commits last, and
     * its record must come second: the log has them in commit order.
     */
    @Test
    void overlappingTransactionsAreRelayedInCommitOrder() throws Exception {
        // Records pending when the engine starts come from its snapshot, in seq order. Wait until it streams.
        writer.write("warm-up");
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> assertThat(received()).contains("warm-up"));

        CountDownLatch inserted = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        // Its own thread, with the application's class loader: the common pool's threads do not have it.
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        ExecutorService thread = Executors.newSingleThreadExecutor();
        try {
            Future<?> slow = thread.submit(() -> {
                Thread.currentThread().setContextClassLoader(appClassLoader);
                writer.writeAndWait("inserted-first", inserted, commit);
                return null;
            });
            // Fails with the writer's own exception if it stops before inserting.
            await().atMost(Duration.ofSeconds(10)).until(() -> inserted.getCount() == 0 || slow.isDone());
            if (slow.isDone()) {
                slow.get();
            }

            writer.write("committed-first");
            commit.countDown();
            slow.get(10, TimeUnit.SECONDS);
        } finally {
            thread.shutdownNow();
        }

        await().atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(received()).contains("committed-first", "inserted-first"));
        List<String> both = received().stream()
                .filter(p -> p.equals("committed-first") || p.equals("inserted-first")).toList();
        assertThat(both).containsExactly("committed-first", "inserted-first");
    }

    @ApplicationScoped
    public static class Writer {

        @Inject
        @Channel("cdc-events")
        OutboxEmitter<String> emitter;

        @Inject
        SessionFactory sessionFactory;

        @Transactional
        public void write(String payload) {
            emitter.sendAndAwait(payload);
        }

        /** Writes the record, says so, and commits only when told to. */
        @Transactional
        public void writeAndWait(String payload, CountDownLatch inserted, CountDownLatch commit) {
            emitter.sendAndAwait(payload);
            sessionFactory.getCurrentSession().flush();
            inserted.countDown();
            try {
                if (!commit.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("not told to commit");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }

        @Transactional
        public long pending() {
            return sessionFactory.getCurrentSession()
                    .createQuery("select count(e) from OutboxRecordEntity e where e.status = 0", Long.class)
                    .getSingleResult();
        }
    }
}
