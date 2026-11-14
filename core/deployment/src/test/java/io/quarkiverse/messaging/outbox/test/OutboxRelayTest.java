package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.runtime.OutboxRelay;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.vertx.core.Vertx;

public class OutboxRelayTest {

    private InMemoryOutboxStore store;
    private TestMapper mapper;
    private OutboxConfig config;

    @BeforeEach
    void setUp() {
        store = new InMemoryOutboxStore(Clock.systemUTC(), "relay-1");
        mapper = new TestMapper();
        config = new OutboxConfig(100, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofMillis(200), Duration.ofSeconds(30), "strict", "delete", 1024);
    }

    private OutboxRelay createRelay() {
        return createRelay(config);
    }

    private OutboxRelay createRelay(OutboxConfig cfg) {
        return new OutboxRelay(store, mapper, List.of(), cfg, Clock.systemUTC(), "test-channel",
                Vertx.vertx());
    }

    private OutboxRecord record(String payload) {
        OutboxRecord r = new OutboxRecord();
        r.setId(UUID.randomUUID());
        r.setChannel("test-channel");
        r.setPayload(payload.getBytes());
        r.setPayloadType("java.lang.String");
        return r;
    }

    // --- R1: records claimed and emitted ---

    @Test
    void r1_claimsAndEmitsRecords() {
        store.addPending(record("msg-0"));
        store.addPending(record("msg-1"));

        OutboxRelay relay = createRelay();
        Multi<Message<?>> publisher = relay.getPublisher();

        AssertSubscriber<Message<?>> sub = publisher
                .subscribe().withSubscriber(AssertSubscriber.create(2));

        relay.signal();

        sub.awaitItems(1, Duration.ofSeconds(5));
        sub.getItems().get(0).ack().toCompletableFuture().join();

        sub.awaitItems(2, Duration.ofSeconds(5));
        assertThat(sub.getItems()).hasSize(2);
    }

    // --- R5: ack calls delivered ---

    @Test
    void r5_ackCallsDelivered() {
        store.addPending(record("msg"));

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(1));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        sub.getItems().get(0).ack().toCompletableFuture().join();
        assertThat(store.deliveredCount).isEqualTo(1);
    }

    // --- R6: nack calls retry ---

    @Test
    void r6_nackCallsRetry() {
        store.addPending(record("msg"));

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(1));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        sub.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();
        assertThat(store.retryCount).isEqualTo(1);
    }

    // --- R7: backoff doubles ---

    @Test
    void r7_backoffDoubles() {
        OutboxRelay relay = createRelay();
        Instant now = Instant.now();
        Instant t1 = relay.computeNextAttempt(1);
        Instant t2 = relay.computeNextAttempt(2);
        Instant t3 = relay.computeNextAttempt(3);

        assertThat(Duration.between(now, t1).toMillis()).isBetween(900L, 1300L);
        assertThat(Duration.between(now, t2).toMillis()).isBetween(1900L, 2500L);
        assertThat(Duration.between(now, t3).toMillis()).isBetween(3900L, 4900L);
    }

    // --- R8: max-attempts parks ---

    @Test
    void r8_parkAfterMaxAttempts() {
        config = new OutboxConfig(100, 3, Duration.ofMillis(1), Duration.ofMinutes(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30), "strict", "delete", 1024);
        store.addPending(record("msg"));

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        relay.signal();

        // Each attempt is a new emission: nack the first delivery and both redeliveries.
        for (int i = 1; i <= 3; i++) {
            sub.awaitItems(i, Duration.ofSeconds(5));
            sub.getItems().get(i - 1).nack(new RuntimeException("fail")).toCompletableFuture().join();
        }

        assertThat(store.parkCount).isEqualTo(1);
    }

    // --- R15: a failing record is logged: first nack, every 10th, and when parked ---

    @Test
    void r15_nacksAndParkAreLoggedAsWarnings() {
        config = new OutboxConfig(100, 12, Duration.ofMillis(1), Duration.ofMillis(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30), "strict", "delete", 1024);
        store.addPending(record("msg"));

        List<LogRecord> warnings = new CopyOnWriteArrayList<>();
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord r) {
                if (r.getLevel().intValue() >= Level.WARNING.intValue()) {
                    warnings.add(r);
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        Logger logger = Logger.getLogger("io.quarkiverse.messaging.outbox.runtime.relay.Dispatcher");
        logger.addHandler(handler);
        try {
            OutboxRelay relay = createRelay();
            AssertSubscriber<Message<?>> sub = relay.getPublisher()
                    .subscribe().withSubscriber(AssertSubscriber.create(20));
            relay.signal();

            for (int i = 1; i <= 12; i++) {
                sub.awaitItems(i, Duration.ofSeconds(5));
                sub.getItems().get(i - 1).nack(new RuntimeException("rejected")).toCompletableFuture().join();
            }
            assertThat(store.parkCount).isEqualTo(1);

            // Attempts 1 and 10 are warnings, the others are debug; parking is a warning.
            assertThat(warnings).extracting(OutboxRelayTest::formatted)
                    .filteredOn(m -> m.contains("was nacked")).hasSize(2)
                    .allMatch(m -> m.contains("The records behind it on the channel wait."));
            assertThat(warnings).extracting(OutboxRelayTest::formatted)
                    .filteredOn(m -> m.contains("parked record")).hasSize(1);
        } finally {
            logger.removeHandler(handler);
        }
    }

    /** The log message with its parameters, whether JBoss LogManager or the JDK backs JBoss Logging. */
    private static String formatted(LogRecord r) {
        try {
            return (String) r.getClass().getMethod("getFormattedMessage").invoke(r);
        } catch (ReflectiveOperationException e) {
            return r.getMessage();
        }
    }

    // --- R12: mapper throws parks record ---

    @Test
    void r12_mapperErrorParksRecord() {
        store.addPending(record("bad"));
        store.addPending(record("good"));

        mapper.failOn = "bad";
        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .select().where(m -> m != null)
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        assertThat(store.parkCount).isEqualTo(1);
    }

    // --- R13: claim failure doesn't reach connector ---

    @Test
    void r13_claimFailureRecoveredSilently() {
        store.addPending(record("msg"));
        store.setFailOnClaim(true);

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        relay.signal();

        // Claim failed, nothing emitted, no error to subscriber
        sub.assertNotTerminated();
        assertThat(sub.getItems()).isEmpty();
    }

    // --- R11: none mode respects max-in-flight ---

    @Test
    void r11_noneOrderingRespectsMaxInFlight() {
        OutboxConfig noneCfg = new OutboxConfig(null, null, 100, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofMillis(200), Duration.ofSeconds(30), "none", "delete", 3, true);

        for (int i = 0; i < 10; i++) {
            store.addPending(record("msg-" + i));
        }

        OutboxRelay relay = createRelay(noneCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(3, Duration.ofSeconds(5));

        // Wait a bit to confirm no more arrive without acking
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
        assertThat(sub.getItems().size()).isEqualTo(3);

        // Ack one, expect one more to be emitted
        sub.getItems().get(0).ack().toCompletableFuture().join();
        sub.awaitItems(4, Duration.ofSeconds(5));
        assertThat(sub.getItems().size()).isEqualTo(4);
    }

    // --- F6: two instances, unordered, disjoint delivery ---

    @Test
    void f6_twoRelaysDeliverDisjointRecords() {
        OutboxConfig noneCfg = new OutboxConfig(null, null, 100, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofMillis(200), Duration.ofSeconds(30), "none", "delete", 1024, true);

        var records = new ArrayList<OutboxRecord>();
        var seqGen = new AtomicLong(1);
        var leaseState = new InMemoryOutboxStore.SharedLeaseState();

        InMemoryOutboxStore store1 = new InMemoryOutboxStore(Clock.systemUTC(), "relay-1", records, seqGen, leaseState);
        InMemoryOutboxStore store2 = new InMemoryOutboxStore(Clock.systemUTC(), "relay-2", records, seqGen, leaseState);

        for (int i = 0; i < 6; i++) {
            store1.addPending(record("msg-" + i));
        }

        Vertx vertx = Vertx.vertx();
        OutboxRelay relay1 = new OutboxRelay(store1, mapper, List.of(), noneCfg, Clock.systemUTC(), "test-channel",
                vertx);
        OutboxRelay relay2 = new OutboxRelay(store2, mapper, List.of(), noneCfg, Clock.systemUTC(), "test-channel",
                vertx);

        AssertSubscriber<Message<?>> sub1 = relay1.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));
        AssertSubscriber<Message<?>> sub2 = relay2.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay1.signal();
        relay2.signal();

        // Wait for all 6 to be delivered across both relays
        try {
            Thread.sleep(3000);
        } catch (InterruptedException ignored) {
        }

        // Ack all messages
        sub1.getItems().forEach(m -> m.ack().toCompletableFuture().join());
        sub2.getItems().forEach(m -> m.ack().toCompletableFuture().join());

        int total = sub1.getItems().size() + sub2.getItems().size();
        assertThat(total).isEqualTo(6);

        // Verify disjoint — collect all payloads
        Set<String> allPayloads = new HashSet<>();
        sub1.getItems().forEach(m -> allPayloads.add((String) m.getPayload()));
        sub2.getItems().forEach(m -> allPayloads.add((String) m.getPayload()));
        assertThat(allPayloads).hasSize(6);
    }

    // --- F8: key ordering, nack one key, other keys continue ---

    @Test
    void f8_keyOrderingNackOneKeyOthersContinue() {
        // The retry of A-1 must come after B-2: a 300 ms backoff leaves time for the ack of B-1 to release B-2.
        OutboxConfig keyCfg = new OutboxConfig(null, null, 100, 3, Duration.ofMillis(300), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofMillis(50), "key", "delete", 1024, true);

        OutboxRecord rA1 = record("A-1");
        rA1.setMsgKey("A");
        OutboxRecord rA2 = record("A-2");
        rA2.setMsgKey("A");
        OutboxRecord rB1 = record("B-1");
        rB1.setMsgKey("B");
        OutboxRecord rB2 = record("B-2");
        rB2.setMsgKey("B");

        store.addPending(rA1);
        store.addPending(rB1);
        store.addPending(rA2);
        store.addPending(rB2);

        OutboxRelay relay = createRelay(keyCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(2, Duration.ofSeconds(5));

        // Should have A-1 and B-1 (one per key)
        Set<String> firstBatch = new HashSet<>();
        sub.getItems().forEach(m -> firstBatch.add((String) m.getPayload()));
        assertThat(firstBatch).containsExactlyInAnyOrder("A-1", "B-1");

        // Nack A-1, ack B-1
        for (Message<?> m : sub.getItems()) {
            if ("A-1".equals(m.getPayload())) {
                m.nack(new RuntimeException("reject A")).toCompletableFuture().join();
            } else {
                m.ack().toCompletableFuture().join();
            }
        }

        // B-2 emitted next (B's queue advances). A-1 retries in place — A-2 must wait.
        sub.awaitItems(3, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(2).getPayload()).isEqualTo("B-2");

        // After retry delay, A-1 is re-emitted
        sub.awaitItems(4, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(3).getPayload()).isEqualTo("A-1");

        // Ack both B-2 and the retried A-1
        sub.getItems().get(2).ack().toCompletableFuture().join();
        sub.getItems().get(3).ack().toCompletableFuture().join();

        // Now A-2 is emitted (A's queue advances after A-1 acked)
        sub.awaitItems(5, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(4).getPayload()).isEqualTo("A-2");
        sub.getItems().get(4).ack().toCompletableFuture().join();
    }

    // --- R9: strict, nack on record 2 of 3 → records 3 waits, record 2 retried first ---

    @Test
    void r9_strictNackBlocksSuccessors() {
        config = new OutboxConfig(100, -1, Duration.ofMillis(1), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofMillis(50), "strict", "delete", 1024);

        store.addPending(record("msg-1"));
        store.addPending(record("msg-2"));
        store.addPending(record("msg-3"));

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(0).getPayload()).isEqualTo("msg-1");
        sub.getItems().get(0).ack().toCompletableFuture().join();

        sub.awaitItems(2, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(1).getPayload()).isEqualTo("msg-2");
        sub.getItems().get(1).nack(new RuntimeException("fail")).toCompletableFuture().join();

        // msg-2 retried in place — msg-3 must NOT appear before msg-2 is acked
        sub.awaitItems(3, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(2).getPayload()).isEqualTo("msg-2");
        sub.getItems().get(2).ack().toCompletableFuture().join();

        sub.awaitItems(4, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(3).getPayload()).isEqualTo("msg-3");
        sub.getItems().get(3).ack().toCompletableFuture().join();
    }

    // --- R17: key, head waiting for retry, queued tail released ---

    @Test
    void r17_keyNackReleasesQueuedTail() {
        OutboxConfig keyCfg = new OutboxConfig(null, null, 100, -1, Duration.ofMillis(1), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofMillis(50), "key", "delete", 1024, true);

        OutboxRecord rA1 = record("A-1");
        rA1.setMsgKey("A");
        OutboxRecord rA2 = record("A-2");
        rA2.setMsgKey("A");

        store.addPending(rA1);
        store.addPending(rA2);

        OutboxRelay relay = createRelay(keyCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(0).getPayload()).isEqualTo("A-1");

        sub.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        // A-2 should have been released (tail released on nack)
        assertThat(store.releaseCount).isGreaterThanOrEqualTo(1);

        // A-1 retried in place
        sub.awaitItems(2, Duration.ofSeconds(5));
        assertThat(sub.getItems().get(1).getPayload()).isEqualTo("A-1");
        sub.getItems().get(1).ack().toCompletableFuture().join();
    }

    // --- Claim renewal: no duplicate emission during long outage ---

    @Test
    void claimNotReEmittedAfterLeaseExpiry() {
        OutboxConfig noneCfg = new OutboxConfig(null, null, 100, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofMillis(200), "none", "delete", 1024, true);

        store.addPending(record("long-outage"));

        OutboxRelay relay = createRelay(noneCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        // Don't ack — simulate a broker outage longer than the lease duration
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }

        // Poll timer fires multiple times during the outage; the record must not be emitted again
        assertThat(sub.getItems()).hasSize(1);

        sub.getItems().get(0).ack().toCompletableFuture().join();
    }

    // --- Outbox id and attempt on relayed messages ---

    @Test
    void outboxMetadata_idAndAttemptOnFirstDelivery() {
        OutboxRecord rec = record("msg");
        store.addPending(rec);

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(1));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        Message<?> msg = sub.getItems().get(0);
        OutboxMetadata meta = msg.getMetadata(OutboxMetadata.class).orElseThrow();
        assertThat(meta.getId()).hasValue(rec.getId());
        assertThat(meta.getAttempt()).isZero();

        msg.ack().toCompletableFuture().join();
    }

    @Test
    void redelivery_sameIdIncreasingAttempt() {
        config = new OutboxConfig(100, 3, Duration.ofMillis(1), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofMillis(50), "strict", "delete", 1024);
        OutboxRecord rec = record("retry-me");
        store.addPending(rec);
        UUID originalId = rec.getId();

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(10));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        Message<?> first = sub.getItems().get(0);
        OutboxMetadata meta1 = first.getMetadata(OutboxMetadata.class).orElseThrow();
        assertThat(meta1.getId()).hasValue(originalId);
        assertThat(meta1.getAttempt()).isZero();

        first.nack(new RuntimeException("fail")).toCompletableFuture().join();

        // After nack+retry, the poll timer re-claims once nextAttemptAt has passed
        sub.awaitItems(2, Duration.ofSeconds(5));

        Message<?> second = sub.getItems().get(1);
        OutboxMetadata meta2 = second.getMetadata(OutboxMetadata.class).orElseThrow();
        assertThat(meta2.getId()).hasValue(originalId);
        assertThat(meta2.getAttempt()).isEqualTo(1);

        second.ack().toCompletableFuture().join();
    }

    // --- Key ordering: several records per key leave nothing counted as in flight ---

    @Test
    void keyOrdering_severalRecordsPerKey_shutdownDoesNotWait() throws Exception {
        OutboxConfig keyCfg = new OutboxConfig(null, null, 100, -1, Duration.ofMillis(1), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofSeconds(30), "key", "delete", 1024, true);
        for (int i = 1; i <= 3; i++) {
            OutboxRecord r = record("A-" + i);
            r.setMsgKey("A");
            store.addPending(r);
        }

        OutboxRelay relay = createRelay(keyCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        for (int i = 1; i <= 3; i++) {
            sub.awaitItems(i, Duration.ofSeconds(5));
            assertThat(sub.getItems().get(i - 1).getPayload()).isEqualTo("A-" + i);
            sub.getItems().get(i - 1).ack().toCompletableFuture().join();
        }

        // Everything is acked, so shutdown has nothing to wait for.
        relay.shutdown().get(2, TimeUnit.SECONDS);
    }

    // --- R14: after a lease loss, a pending retry is not emitted by this relay ---

    @Test
    void r14_leaseLost_pendingRetryIsNotEmitted() throws Exception {
        config = new OutboxConfig(100, -1, Duration.ofMillis(300), Duration.ofMinutes(1),
                Duration.ofMillis(100), Duration.ofSeconds(30), "strict", "delete", 1024);
        store.addPending(record("msg-1"));
        store.addPending(record("msg-2"));

        OutboxRelay relay = createRelay();
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        sub.awaitItems(1, Duration.ofSeconds(5));

        // The nack schedules a retry of msg-1 about 300 ms later.
        sub.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        // Another relay takes the lease before the retry fires. The next poll notices it.
        store.setLeaseLost(true);
        Thread.sleep(800);

        assertThat(sub.getItems()).hasSize(1);
    }

    // --- A backlog is drained without waiting for the poll timer ---

    @Test
    void backlogDrainsWithoutWaitingForPollTimer() {
        // Poll interval of 10 s: only refills triggered by acks can deliver 30 records in time.
        OutboxConfig noneCfg = new OutboxConfig(null, null, 10, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofSeconds(10), Duration.ofSeconds(30), "none", "delete", 10, true);
        for (int i = 0; i < 30; i++) {
            store.addPending(record("msg-" + i));
        }

        OutboxRelay relay = createRelay(noneCfg);
        AssertSubscriber<Message<?>> sub = relay.getPublisher()
                .subscribe().withSubscriber(AssertSubscriber.create(100));

        relay.signal();
        // Three batches of 10. Acking a batch triggers the next claim immediately.
        for (int batch = 0; batch < 3; batch++) {
            int expected = (batch + 1) * 10;
            sub.awaitItems(expected, Duration.ofSeconds(5));
            for (int i = batch * 10; i < expected; i++) {
                sub.getItems().get(i).ack().toCompletableFuture().join();
            }
        }
        assertThat(store.deliveredCount).isEqualTo(30);
    }

    // --- Test helpers ---

    static class TestMapper implements OutboxRecordMapper {
        String failOn;

        @Override
        public OutboxRecord toRecord(String channel, Message<?> message) {
            throw new UnsupportedOperationException("not needed in relay tests");
        }

        @Override
        public Message<?> toMessage(OutboxRecord record, Type payloadType) {
            String payload = new String(record.getPayload());
            if (payload.equals(failOn)) {
                throw new RuntimeException("mapper error on " + payload);
            }
            return Message.of(payload);
        }
    }
}
