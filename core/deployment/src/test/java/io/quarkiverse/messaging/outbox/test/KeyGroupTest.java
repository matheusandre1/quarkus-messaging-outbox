package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.runtime.relay.KeyGroup;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;

class KeyGroupTest {

    Instant now = Instant.parse("2026-01-01T00:00:00Z");
    final List<String> sent = new ArrayList<>();
    Instant scheduledAt;
    Runnable scheduled;
    KeyGroup group;

    @BeforeEach
    void setUp() {
        // The test moves time forward by changing `now`, and runs the scheduled retry by hand.
        group = new KeyGroup("A", () -> now,
                (when, action) -> {
                    scheduledAt = when;
                    scheduled = action;
                },
                (g, record) -> sent.add(new String(record.getPayload())));
    }

    private static OutboxRecord record(String payload) {
        OutboxRecord r = new OutboxRecord();
        r.setId(UUID.randomUUID());
        r.setPayload(payload.getBytes());
        return r;
    }

    @Test
    void sendsOneRecordAtATime() {
        group.add(record("a"));
        group.add(record("b"));
        group.add(record("c"));

        group.start();
        assertThat(sent).containsExactly("a");

        group.start(); // already busy: nothing more goes out
        assertThat(sent).containsExactly("a");

        group.headSettled();
        assertThat(sent).containsExactly("a", "b");

        group.headSettled();
        group.headSettled();
        assertThat(sent).containsExactly("a", "b", "c");
        assertThat(group.isEmpty()).isTrue();
    }

    @Test
    void failedRecordKeepsItsPlaceAndIsRetriedWhenDue() {
        OutboxRecord a = record("a");
        group.add(a);
        group.add(record("b"));
        group.start();

        a.setNextAttemptAt(now.plus(Duration.ofSeconds(5)));
        group.headRetrying();

        // Nothing is sent while "a" waits, and "b" does not overtake it.
        assertThat(sent).containsExactly("a");
        assertThat(scheduledAt).isEqualTo(now.plus(Duration.ofSeconds(5)));

        now = now.plus(Duration.ofSeconds(5));
        scheduled.run();
        assertThat(sent).containsExactly("a", "a");

        group.headSettled();
        assertThat(sent).containsExactly("a", "a", "b");
    }

    @Test
    void recordThatIsNotDueYetWaitsBeforeItsFirstSend() {
        OutboxRecord a = record("a");
        a.setNextAttemptAt(now.plus(Duration.ofSeconds(3)));
        group.add(a);

        group.start();
        assertThat(sent).isEmpty();

        now = now.plus(Duration.ofSeconds(3));
        scheduled.run();
        assertThat(sent).containsExactly("a");
    }

    @Test
    void removeTailKeepsTheHeadRecord() {
        group.add(record("a"));
        group.add(record("b"));
        group.add(record("c"));
        group.start();

        List<OutboxRecord> tail = group.removeTail();

        assertThat(tail).extracting(r -> new String(r.getPayload())).containsExactly("b", "c");
        assertThat(group.size()).isEqualTo(1);

        group.headSettled();
        assertThat(group.isEmpty()).isTrue();
        assertThat(sent).containsExactly("a");
    }
}
