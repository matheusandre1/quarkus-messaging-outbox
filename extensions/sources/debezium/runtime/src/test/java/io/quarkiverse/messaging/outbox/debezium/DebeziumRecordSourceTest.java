package io.quarkiverse.messaging.outbox.debezium;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.json.JsonObject;

/** The source's rules, with a fake engine, store and relay. */
class DebeziumRecordSourceTest {

    ScheduledExecutorService relayContext;
    FakeStore store;
    FakeEngine engine;
    FakeSink sink;
    DebeziumRecordSource source;
    final AtomicBoolean lostAccess = new AtomicBoolean();
    final AtomicReference<Throwable> failed = new AtomicReference<>();

    void start(boolean unordered) {
        relayContext = Executors.newSingleThreadScheduledExecutor();
        store = new FakeStore();
        engine = new FakeEngine();
        sink = new FakeSink();
        OutboxRecordSourceContext context = new OutboxRecordSourceContext("orders", null, store,
                java.time.Clock.systemUTC(), relayContext, new OutboxRecordSourceContext.Scheduler() {
                    @Override
                    public Cancellable schedule(Duration delay, Runnable action) {
                        var f = relayContext.schedule(action, delay.toMillis(), TimeUnit.MILLISECONDS);
                        return () -> f.cancel(false);
                    }

                    @Override
                    public Cancellable schedulePeriodically(Duration period, Runnable action) {
                        var f = relayContext.scheduleAtFixedRate(action, period.toMillis(), period.toMillis(),
                                TimeUnit.MILLISECONDS);
                        return () -> f.cancel(false);
                    }
                });
        // A 300 ms lease is renewed every 100 ms.
        source = new DebeziumRecordSource(context, Duration.ofMillis(300), false, unordered, engine, new Properties());
        onContext(() -> {
            source.start(sink, new OutboxRecordSource.Listener() {
                @Override
                public void lostAccess() {
                    lostAccess.set(true);
                }

                @Override
                public void failed(Throwable failure) {
                    failed.set(failure);
                }
            });
            return null;
        });
        await().atMost(Duration.ofSeconds(5)).until(() -> engine.handler.get() != null);
    }

    @BeforeEach
    void setUp() {
        start(false);
    }

    @AfterEach
    void tearDown() {
        onContext(() -> {
            source.stop();
            return null;
        });
        relayContext.shutdownNow();
    }

    @Test
    void theEngineMovesOnOnlyWhenEveryRecordOfTheBatchIsSettled() {
        CompletableFuture<Void> batch = engine.send(insert("a", null), insert("b", null));
        await().until(() -> sink.accepted.size() == 2);
        assertThat(payloads(sink.accepted)).containsExactly("a", "b");

        delivered(sink.accepted.get(0));
        assertThat(batch).isNotDone();
        delivered(sink.accepted.get(1));
        await().untilAsserted(() -> assertThat(batch).isCompleted());
        assertThat(store.deleted).hasSize(2);
    }

    @Test
    void aParkedRecordIsSettledAndAFailedOneIsNot() {
        CompletableFuture<Void> batch = engine.send(insert("a", null), insert("b", null));
        await().until(() -> sink.accepted.size() == 2);

        onContext(() -> source.failed(sink.accepted.get(0), Instant.now(), new RuntimeException("nack")))
                .await().indefinitely();
        onContext(() -> source.parked(sink.accepted.get(1), new RuntimeException("gave up"))).await().indefinitely();
        assertThat(batch).isNotDone();
        assertThat(store.updates).isZero(); // nothing written for a failed attempt

        delivered(sink.accepted.get(0));
        await().untilAsserted(() -> assertThat(batch).isCompleted());
        assertThat(store.parked).hasSize(1);
    }

    @Test
    void stoppingAbandonsTheBatchSoTheEngineReadsItAgain() {
        CompletableFuture<Void> batch = engine.send(insert("a", null));
        await().until(() -> sink.accepted.size() == 1);

        onContext(() -> {
            source.stop();
            return null;
        });
        await().untilAsserted(() -> assertThat(batch).isCompletedExceptionally());
        await().until(() -> engine.closed);
    }

    @Test
    void recordsWaitForCapacityInCommitOrder() {
        sink.capacity = 1;
        engine.send(insert("a", null), insert("b", null), insert("c", null));
        await().until(() -> sink.accepted.size() == 1);

        delivered(sink.accepted.get(0));
        await().until(() -> sink.accepted.size() == 2);
        delivered(sink.accepted.get(1));
        await().until(() -> sink.accepted.size() == 3);
        assertThat(payloads(sink.accepted)).containsExactly("a", "b", "c");
    }

    @Test
    void underKeyOrderingHandedBackRecordsWaitWithLaterOnesOfTheirKey() {
        sink.capacity = 3;
        engine.send(insert("k1-a", "k1"), insert("k1-b", "k1"), insert("k2-a", "k2"), insert("k1-c", "k1"),
                insert("k2-b", "k2"));
        await().until(() -> sink.accepted.size() == 3);

        // The relay nacks k1-a and hands back k1-b, as the dispatcher does under ordering=key.
        OutboxRecord k1b = sink.accepted.get(1);
        sink.busy.add("k1");
        sink.settled(k1b);
        onContext(() -> {
            source.released(List.of(k1b));
            return null;
        });
        // Room frees up: k2-b goes out, k1-c waits behind k1-b.
        delivered(sink.accepted.get(2));
        await().until(() -> payloads(sink.accepted).contains("k2-b"));
        assertThat(payloads(sink.accepted)).doesNotContain("k1-c").containsOnlyOnce("k1-b");

        // Deliver k1-a (the retried head) and k2-b to free room.
        delivered(sink.accepted.get(0)); // k1-a
        delivered(sink.accepted.stream().filter(r -> new String(r.getPayload()).equals("k2-b")).findFirst().orElseThrow());

        // Done with k1: its records come back in order.
        sink.busy.remove("k1");
        onContext(() -> {
            source.capacityAvailable();
            return null;
        });
        await().until(() -> payloads(sink.accepted).contains("k1-c"));
        List<String> k1 = payloads(sink.accepted).stream().filter(p -> p.startsWith("k1")).toList();
        assertThat(k1).containsExactly("k1-a", "k1-b", "k1-b", "k1-c");
    }

    @Test
    void losingTheLeaseStopsTheEngineAndTellsTheRelay() {
        CompletableFuture<Void> batch = engine.send(insert("a", null));
        await().until(() -> sink.accepted.size() == 1);

        store.leaseHeld = false;
        await().atMost(Duration.ofSeconds(5)).until(lostAccess::get);
        await().untilAsserted(() -> assertThat(batch).isCompletedExceptionally());
        await().until(() -> engine.closed);
    }

    @Test
    void anEngineFailureIsReportedToTheRelay() {
        engine.failure.get().accept(new IllegalStateException("slot dropped"));
        await().untilAsserted(() -> assertThat(failed.get()).hasMessage("slot dropped"));
    }

    @Test
    void underNoOrderingAHandedBackRecordComesBackWhenItsRetryIsDue() {
        tearDown();
        start(true);
        engine.send(insert("a", "k1"));
        await().until(() -> sink.accepted.size() == 1);

        // The relay nacks it and hands it back, due in 300 ms, as the dispatcher does under ordering=none.
        OutboxRecord a = sink.accepted.get(0);
        a.setNextAttemptAt(Instant.now().plusMillis(300));
        sink.settled(a);
        onContext(() -> {
            source.released(List.of(a));
            return null;
        });
        await().pollDelay(Duration.ofMillis(150)).until(() -> true);
        assertThat(sink.accepted).hasSize(1);
        await().atMost(Duration.ofSeconds(2)).until(() -> sink.accepted.size() == 2);
    }

    // --- Helpers ---

    /** Reports the outcome on the relay's context, and waits for it here: the source completes it on the context. */
    private void delivered(OutboxRecord record) {
        onContext(() -> source.delivered(record)).await().indefinitely();
    }

    private <T> T onContext(Supplier<T> action) {
        CompletableFuture<T> result = new CompletableFuture<>();
        relayContext.execute(() -> {
            try {
                result.complete(action.get());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result.join();
    }

    private static List<String> payloads(List<OutboxRecord> records) {
        return records.stream().map(r -> new String(r.getPayload())).toList();
    }

    private static String insert(String payload, String key) {
        JsonObject row = ChangeEventDecoderTest.row("orders", 0)
                .put("outbox_id", UUID.randomUUID().toString())
                .put("msg_key", key)
                .put("payload", Base64.getEncoder().encodeToString(payload.getBytes()));
        return ChangeEventDecoderTest.event("c", null, row);
    }

    /** Hands batches to the source from an engine thread, like Debezium does. */
    static class FakeEngine implements EngineLauncher {
        final AtomicReference<BatchHandler> handler = new AtomicReference<>();
        final AtomicReference<Consumer<Throwable>> failure = new AtomicReference<>();
        volatile boolean closed;

        @Override
        public Closeable launch(Properties properties, BatchHandler handler, Consumer<Throwable> onFailure) {
            this.handler.set(handler);
            this.failure.set(onFailure);
            return () -> closed = true;
        }

        /** Completes when the source lets the engine move past the batch, exceptionally when it abandons it. */
        CompletableFuture<Void> send(String... values) {
            return CompletableFuture.runAsync(() -> {
                try {
                    handler.get().handle(List.of(values));
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }
    }

    static class FakeSink implements OutboxRecordSink {
        final List<OutboxRecord> accepted = new CopyOnWriteArrayList<>();
        final Set<String> busy = ConcurrentHashMap.newKeySet();
        final Set<UUID> held = ConcurrentHashMap.newKeySet();
        volatile int capacity = 100;

        @Override
        public int remainingCapacity() {
            return capacity - held.size();
        }

        @Override
        public Set<String> busyGroups() {
            return busy;
        }

        @Override
        public List<UUID> heldIds() {
            return new ArrayList<>(held);
        }

        @Override
        public void accept(List<OutboxRecord> records) {
            for (OutboxRecord r : records) {
                accepted.add(r);
                held.add(r.getId());
            }
        }

        void settled(OutboxRecord r) {
            held.remove(r.getId());
        }
    }

    class FakeStore implements OutboxStore {
        volatile boolean leaseHeld = true;
        final List<UUID> deleted = new CopyOnWriteArrayList<>();
        final List<UUID> parked = new CopyOnWriteArrayList<>();
        volatile int updates;

        @Override
        public Uni<Boolean> holdLease(Duration lease, boolean acquire) {
            return Uni.createFrom().item(lease.isZero() || leaseHeld);
        }

        @Override
        public Uni<Void> delivered(OutboxRecord record, boolean keep) {
            deleted.add(record.getId());
            sink.settled(record);
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> park(OutboxRecord record, Throwable failure) {
            parked.add(record.getId());
            sink.settled(record);
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure) {
            updates++;
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> persist(OutboxRecord record, Runnable afterCommit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<List<OutboxRecord>> claim(ClaimRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<Void> release(List<OutboxRecord> records) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<Void> renewClaims(List<UUID> recordIds, Duration lease) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<OutboxStats> stats() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Uni<Integer> purge(Instant sentBefore) {
            throw new UnsupportedOperationException();
        }
    }
}
