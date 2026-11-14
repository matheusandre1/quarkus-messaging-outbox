package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.runtime.OutboxRelay;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;

/** What the relay does for a custom record source: restart after a failure, timers, threads. */
class OutboxRecordSourceSpiTest {

    final Vertx vertx = Vertx.vertx();

    @AfterEach
    void close() {
        vertx.close().toCompletionStage().toCompletableFuture().join();
    }

    static OutboxConfig config() {
        // 50 ms first restart delay
        return new OutboxConfig(100, -1, Duration.ofMillis(50), Duration.ofMillis(200), Duration.ofSeconds(5),
                Duration.ofSeconds(30), "strict", "delete", 1024);
    }

    static OutboxRecord record(String payload) {
        OutboxRecord r = new OutboxRecord();
        r.setId(UUID.randomUUID());
        r.setChannel("spi-channel");
        r.setPayload(payload.getBytes());
        r.setPayloadType("java.lang.String");
        return r;
    }

    /**
     * Fails on its first start, from a timer of the context. Hands out one record on the next start. Its outcome
     * Unis complete on another thread, which the relay must handle.
     */
    static class FlakySource implements OutboxRecordSource {
        final OutboxRecordSourceContext context;
        final AtomicInteger starts = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final List<String> delivered = new CopyOnWriteArrayList<>();
        final AtomicReference<String> deliveredThread = new AtomicReference<>();

        FlakySource(OutboxRecordSourceContext context) {
            this.context = context;
        }

        @Override
        public void start(OutboxRecordSink sink, Listener listener) {
            if (starts.incrementAndGet() == 1) {
                context.schedule(Duration.ofMillis(10), () -> listener.failed(new IllegalStateException("log lost")));
            } else {
                sink.accept(List.of(record("after-restart")));
            }
        }

        @Override
        public void capacityAvailable() {
        }

        @Override
        public Uni<Void> delivered(OutboxRecord record) {
            delivered.add(new String(record.getPayload()));
            return Uni.createFrom().voidItem().emitOn(Infrastructure.getDefaultWorkerPool());
        }

        @Override
        public Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> parked(OutboxRecord record, Throwable failure) {
            return Uni.createFrom().voidItem();
        }

        @Override
        public void released(List<OutboxRecord> records) {
        }

        @Override
        public void stop() {
        }

        @Override
        public Uni<Void> close(List<OutboxRecord> unsent) {
            closes.incrementAndGet();
            return Uni.createFrom().voidItem();
        }
    }

    @Test
    void aFailedSourceIsClosedReportedAndStartedAgain() {
        AtomicReference<FlakySource> created = new AtomicReference<>();
        OutboxRelay relay = new OutboxRelay(new InMemoryOutboxStore(Clock.systemUTC(), "relay"),
                new OutboxRelayTest.TestMapper(), List.of(), config(), Clock.systemUTC(), "spi-channel", vertx,
                context -> {
                    FlakySource source = new FlakySource(context);
                    created.set(source);
                    return source;
                }, null);
        AssertSubscriber<Message<?>> sub = relay.getPublisher().subscribe().withSubscriber(AssertSubscriber.create(10));

        await().atMost(Duration.ofSeconds(5)).until(() -> relay.getSourceFailure().isPresent()
                || created.get().starts.get() >= 2);
        sub.awaitItems(1, Duration.ofSeconds(5));
        assertThat(created.get().starts).hasValue(2);
        assertThat(created.get().closes).hasValue(1);
        assertThat(relay.getSourceFailure()).isEmpty();

        // The source's Uni completes on a worker thread: the relay still settles the record on its own context.
        sub.getItems().get(0).ack().toCompletableFuture().join();
        await().until(() -> created.get().delivered.contains("after-restart"));
        relay.shutdown().join();
        assertThat(created.get().closes).hasValue(2);
    }

    @Test
    void contextTimersRunOnTheRelaysContextAndCanBeCancelled() {
        AtomicReference<OutboxRecordSourceContext> captured = new AtomicReference<>();
        OutboxRelay relay = new OutboxRelay(new InMemoryOutboxStore(Clock.systemUTC(), "relay"),
                new OutboxRelayTest.TestMapper(), List.of(), config(), Clock.systemUTC(), "spi-channel", vertx,
                context -> {
                    captured.set(context);
                    return new FlakySource(context) {
                        @Override
                        public void start(OutboxRecordSink sink, Listener listener) {
                        }
                    };
                }, null);
        relay.getPublisher().subscribe().withSubscriber(AssertSubscriber.create(10));

        AtomicInteger ticks = new AtomicInteger();
        AtomicReference<Boolean> onContext = new AtomicReference<>();
        var periodic = captured.get().schedulePeriodically(Duration.ofMillis(10), () -> {
            ticks.incrementAndGet();
            onContext.set(io.vertx.core.Vertx.currentContext() != null);
        });
        await().until(() -> ticks.get() >= 3);
        periodic.cancel();
        int afterCancel = ticks.get();
        AtomicInteger once = new AtomicInteger();
        captured.get().schedule(Duration.ofMillis(10), once::incrementAndGet).cancel();

        await().pollDelay(Duration.ofMillis(100)).until(() -> true);
        assertThat(ticks.get()).isLessThanOrEqualTo(afterCancel + 1);
        assertThat(once).hasValue(0);
        assertThat(onContext.get()).isTrue();
        relay.shutdown().join();
    }
}
