package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.runtime.relay.Dispatcher;
import io.quarkiverse.messaging.outbox.runtime.relay.OrderingPolicy;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;

/**
 * The dispatcher on its own: no store, no Vert.x. Actions run inline, and timers fire when the test says so.
 */
class DispatcherTest {

    /** Remembers what the dispatcher reported. */
    static class FakeSource implements OutboxRecordSource {
        final List<String> events = new ArrayList<>();

        @Override
        public Uni<Void> delivered(OutboxRecord record) {
            events.add("delivered " + payload(record));
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure) {
            events.add("failed " + payload(record));
            return Uni.createFrom().voidItem();
        }

        @Override
        public Uni<Void> parked(OutboxRecord record, Throwable failure) {
            events.add("parked " + payload(record));
            return Uni.createFrom().voidItem();
        }

        @Override
        public void released(List<OutboxRecord> records) {
            records.forEach(r -> events.add("released " + payload(r)));
        }

        @Override
        public void capacityAvailable() {
        }

        @Override
        public void start(OutboxRecordSink sink, Listener listener) {
        }

        @Override
        public void stop() {
        }

        @Override
        public Uni<Void> close(List<OutboxRecord> unsent) {
            return Uni.createFrom().voidItem();
        }
    }

    static class StringMapper implements OutboxRecordMapper {
        @Override
        public OutboxRecord toRecord(String channel, Message<?> message) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Message<?> toMessage(OutboxRecord record, Type payloadType) {
            return Message.of(payload(record));
        }
    }

    static String payload(OutboxRecord record) {
        return new String(record.getPayload());
    }

    static OutboxRecord record(String payload, String key) {
        OutboxRecord r = new OutboxRecord();
        r.setId(UUID.randomUUID());
        r.setChannel("test-channel");
        r.setPayload(payload.getBytes());
        r.setMsgKey(key);
        return r;
    }

    FakeSource source;
    List<Runnable> timers;
    AssertSubscriber<Message<?>> downstream;

    @BeforeEach
    void setUp() {
        source = new FakeSource();
        timers = new ArrayList<>();
    }

    private Dispatcher dispatcher(OrderingPolicy policy, int maxAttempts) {
        // A 500 ms backoff keeps a retry in the future long enough for the test to see it scheduled.
        OutboxConfig config = new OutboxConfig(100, maxAttempts, Duration.ofMillis(500), Duration.ofMinutes(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30), "unused", "delete", 1024);
        Dispatcher dispatcher = new Dispatcher(policy, config, new StringMapper(), List.of(), Clock.systemUTC(),
                "test-channel", Runnable::run, (delayMs, action) -> timers.add(action), source);
        downstream = Multi.createFrom().<Message<?>> emitter(dispatcher::start)
                .subscribe().withSubscriber(AssertSubscriber.create(100));
        return dispatcher;
    }

    private List<String> sent() {
        return downstream.getItems().stream().map(m -> String.valueOf(m.getPayload())).toList();
    }

    @Test
    void keyOrdering_oneRecordPerKeyAtATime() {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.KEY, -1);

        dispatcher.accept(List.of(record("A-1", "A"), record("B-1", "B"), record("A-2", "A")));
        assertThat(sent()).containsExactly("A-1", "B-1");

        downstream.getItems().get(0).ack().toCompletableFuture().join();

        assertThat(sent()).containsExactly("A-1", "B-1", "A-2");
        assertThat(source.events).containsExactly("delivered A-1");
    }

    @Test
    void strictOrdering_nackedRecordIsRetriedBeforeTheNextOne() throws Exception {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.STRICT, -1);

        dispatcher.accept(List.of(record("m-1", null), record("m-2", null)));
        downstream.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        // The retry is scheduled, and m-2 has not been sent.
        assertThat(sent()).containsExactly("m-1");
        assertThat(source.events).containsExactly("failed m-1");
        assertThat(timers).hasSize(1);

        Thread.sleep(700); // let the backoff pass, jitter included
        timers.get(0).run();
        assertThat(sent()).containsExactly("m-1", "m-1");

        downstream.getItems().get(1).ack().toCompletableFuture().join();
        assertThat(sent()).containsExactly("m-1", "m-1", "m-2");
    }

    @Test
    void keyOrdering_nackGivesBackTheRestOfTheGroup() {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.KEY, -1);

        dispatcher.accept(List.of(record("A-1", "A"), record("A-2", "A")));
        downstream.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        assertThat(source.events).containsExactly("failed A-1", "released A-2");
        assertThat(dispatcher.busyGroups()).containsExactly("A");
    }

    @Test
    void noOrdering_nackGivesTheRecordBack() {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.NONE, -1);

        dispatcher.accept(List.of(record("m-1", "A"), record("m-2", "A")));
        assertThat(sent()).containsExactly("m-1", "m-2");

        downstream.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        assertThat(source.events).containsExactly("failed m-1", "released m-1");
        assertThat(dispatcher.heldIds()).hasSize(1);
    }

    @Test
    void maxAttemptsParksTheRecordAndMovesOn() {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.STRICT, 1);

        dispatcher.accept(List.of(record("m-1", null), record("m-2", null)));
        downstream.getItems().get(0).nack(new RuntimeException("fail")).toCompletableFuture().join();

        assertThat(source.events).containsExactly("parked m-1");
        assertThat(sent()).containsExactly("m-1", "m-2");
    }

    @Test
    void droppedRecordsIgnoreLateOutcomes() {
        Dispatcher dispatcher = dispatcher(OrderingPolicy.STRICT, -1);
        dispatcher.accept(List.of(record("m-1", null), record("m-2", null)));

        List<OutboxRecord> dropped = dispatcher.dropAll();
        assertThat(dropped).hasSize(2);
        assertThat(dispatcher.heldIds()).isEmpty();

        // A late ack still removes the record from the outbox, but nothing more is sent.
        downstream.getItems().get(0).ack().toCompletableFuture().join();
        assertThat(source.events).containsExactly("delivered m-1");
        assertThat(sent()).containsExactly("m-1");
    }
}
