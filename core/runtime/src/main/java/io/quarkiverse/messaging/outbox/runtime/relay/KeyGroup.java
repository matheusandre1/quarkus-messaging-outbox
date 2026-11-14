package io.quarkiverse.messaging.outbox.runtime.relay;

import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import io.quarkiverse.messaging.outbox.spi.OutboxRecord;

/**
 * The records of one ordering key, in the order they must be sent.
 * <p>
 * A group emits one record at a time. Only the head record is ever emitted: the next one goes only after the
 * head is settled, and a failed head is retried in place.
 * With strict ordering there is a single group for the whole channel.
 * <p>
 * Not thread-safe: a group is used from the relay's context only.
 */
public final class KeyGroup {

    /** Receives the records a group emits. */
    @FunctionalInterface
    public interface GroupListener {

        /**
         * Sends the record downstream. The outcome is reported later with {@link KeyGroup#headSettled()} or
         * {@link KeyGroup#headRetrying()}.
         */
        void emit(KeyGroup group, OutboxRecord record);
    }

    /** Runs an action later, used to wait for a retry. */
    @FunctionalInterface
    public interface RetryScheduler {

        void runAt(Instant when, Runnable action);
    }

    private final String name;
    private final InstantSource clock;
    private final RetryScheduler scheduler;
    private final GroupListener listener;
    private final ArrayDeque<OutboxRecord> records = new ArrayDeque<>();
    /** The head record has been emitted or is waiting for its retry. */
    private boolean busy;

    public KeyGroup(String name, InstantSource clock, RetryScheduler scheduler, GroupListener listener) {
        this.name = name;
        this.clock = clock;
        this.scheduler = scheduler;
        this.listener = listener;
    }

    public String name() {
        return name;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public int size() {
        return records.size();
    }

    /** Adds a record at the back. */
    public void add(OutboxRecord record) {
        records.add(record);
    }

    /** Emits the head record, unless it is already in flight or waiting for its retry. */
    public void start() {
        if (busy || records.isEmpty()) {
            return;
        }
        busy = true;
        emitHead();
    }

    /** The head record was delivered or parked: advance to the next one. */
    public void headSettled() {
        records.poll();
        busy = false;
        start();
    }

    /** The head record failed and has a new attempt time: it keeps its place and is emitted again when due. */
    public void headRetrying() {
        emitHead();
    }

    /** Removes and returns every record behind the head. */
    public List<OutboxRecord> removeTail() {
        if (records.size() <= 1) {
            return List.of();
        }
        OutboxRecord head = records.poll();
        List<OutboxRecord> tail = new ArrayList<>(records);
        records.clear();
        records.add(head);
        return tail;
    }

    private void emitHead() {
        OutboxRecord head = records.peek();
        if (head == null) {
            busy = false;
            return;
        }
        Instant nextAttempt = head.getNextAttemptAt();
        if (nextAttempt != null && nextAttempt.isAfter(clock.instant())) {
            scheduler.runAt(nextAttempt, this::emitHead);
            return;
        }
        listener.emit(this, head);
    }
}
