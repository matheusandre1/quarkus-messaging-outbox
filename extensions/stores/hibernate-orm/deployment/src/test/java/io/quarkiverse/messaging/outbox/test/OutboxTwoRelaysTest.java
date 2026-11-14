package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.runtime.OutboxRelay;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkus.test.QuarkusExtensionTest;
import io.vertx.core.Vertx;

/**
 * Two application instances relaying the same channel from one database, simulated with two relays that
 * each have their own store and relay id.
 */
public class OutboxTwoRelaysTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(
                    HibernateOrmStoreTest.TestService.class, CollectorConnector.class, SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.test-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    HibernateOrmStoreTest.TestService service;

    @Inject
    Vertx vertx;

    /** What each relay delivered, as "relay:payload", in delivery order across both relays. */
    final List<String> delivered = new CopyOnWriteArrayList<>();
    final List<OutboxRelay> relays = new ArrayList<>();

    @BeforeEach
    void clean() {
        service.deleteAll();
        delivered.clear();
    }

    @AfterEach
    void stopRelays() throws Exception {
        for (OutboxRelay relay : relays) {
            relay.shutdown().get(10, TimeUnit.SECONDS);
        }
        relays.clear();
    }

    private static OutboxConfig config(String ordering, int batchSize) {
        // 1 s lease, 100 ms poll: a standby relay notices a released lease within about 330 ms.
        return new OutboxConfig(null, null, batchSize, -1, Duration.ofMillis(100), Duration.ofSeconds(1),
                Duration.ofMillis(100), Duration.ofSeconds(1), ordering, "delete", 1024, true);
    }

    /** Starts a relay with its own store, acking every message and recording who delivered it. */
    private OutboxRelay start(String name, OutboxConfig cfg) {
        OutboxStore store = service.createStoreWithId(name);
        OutboxRelay relay = new OutboxRelay(store, new SimpleStringMapper(), List.of(), cfg, Clock.systemUTC(),
                "test-channel", vertx);
        relays.add(relay);
        relay.getPublisher().subscribe().with(message -> {
            delivered.add(name + ":" + payload(message));
            message.ack();
        });
        return relay;
    }

    private static String payload(Message<?> message) {
        Object payload = message.getPayload();
        return payload instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(payload);
    }

    private void write(String prefix, int count) {
        OutboxStore writer = service.createStore();
        for (int i = 0; i < count; i++) {
            service.persistInTransaction(writer, prefix + "-" + i);
        }
    }

    private static List<String> payloads(List<String> entries) {
        return entries.stream().map(e -> e.substring(e.indexOf(':') + 1)).toList();
    }

    private static List<String> expected(String prefix, int count) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            list.add(prefix + "-" + i);
        }
        return list;
    }

    @Test
    void strictOrdering_oneRelayAtATime_secondTakesOverInOrder() throws Exception {
        OutboxRelay a = start("relay-a", config("strict", 5));
        OutboxRelay b = start("relay-b", config("strict", 5));

        write("first", 10);
        await().atMost(Duration.ofSeconds(15)).until(() -> delivered.size() >= 10);

        // While both run, only the lease holder sends, in order.
        String holder = delivered.get(0).substring(0, delivered.get(0).indexOf(':'));
        assertThat(delivered).allMatch(e -> e.startsWith(holder + ":"));
        assertThat(payloads(delivered)).containsExactlyElementsOf(expected("first", 10));

        // The holder shuts down and gives up its lease. The other one takes over.
        OutboxRelay stopping = holder.equals("relay-a") ? a : b;
        stopping.shutdown().get(10, TimeUnit.SECONDS);
        relays.remove(stopping);

        write("second", 10);
        await().atMost(Duration.ofSeconds(15)).until(() -> delivered.size() >= 20);

        List<String> afterTakeover = delivered.subList(10, delivered.size());
        assertThat(afterTakeover).noneMatch(e -> e.startsWith(holder + ":"));
        assertThat(payloads(afterTakeover)).containsExactlyElementsOf(expected("second", 10));
    }

    @Test
    void noOrdering_bothRelaysWork_eachRecordDeliveredOnce() {
        start("relay-a", config("none", 5));
        start("relay-b", config("none", 5));

        write("msg", 60);
        await().atMost(Duration.ofSeconds(20)).until(() -> delivered.size() >= 60);

        // Give a duplicate the chance to show up before checking.
        await().pollDelay(Duration.ofMillis(500)).until(() -> true);
        assertThat(payloads(delivered)).hasSize(60).doesNotHaveDuplicates()
                .containsExactlyInAnyOrderElementsOf(expected("msg", 60));
    }
}
