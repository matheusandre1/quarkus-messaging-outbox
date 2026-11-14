package io.quarkiverse.messaging.outbox.debezium;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.Cancellable;

/**
 * Reads outbox records from the database's transaction log with the Debezium embedded engine.
 * <p>
 * Records arrive in commit order, as soon as their transaction commits. The rest works as with the polling
 * source: the relay orders, sends and retries them, and this source records the outcome in the outbox table.
 *
 * <h2>Progress</h2>
 * The engine's position may move past a batch only when every record of it is delivered or parked:
 * {@link #onBatch} blocks the engine thread until then. If the source stops or loses the lease first, the batch
 * is abandoned and read again later. The outbox table stays the durable progress: a record is deleted, or marked
 * sent, once delivered, and the engine's snapshot at start finds the ones that were not.
 *
 * <h2>One reader</h2>
 * A replication slot has one reader, so the source holds the channel lease and runs the engine only while it
 * holds it, whatever the ordering mode. Another instance takes over when the lease expires.
 *
 * <h2>Records handed back</h2>
 * The log is a stream, so records the relay hands back are held here, as the SPI requires: under
 * {@code ordering=none} until their retry is due, under {@code ordering=key} with any later record of the same
 * key until the relay is done with that key.
 *
 * <h2>Failures</h2>
 * If the engine stops with an error, the source reports it with {@link Listener#failed}. The relay restarts it,
 * which starts a new engine once the lease is held again.
 *
 * <h2>Threads</h2>
 * All state is used on the relay's context. The engine thread only hands batches over and waits.
 */
public final class DebeziumRecordSource implements OutboxRecordSource {

    private static final Logger log = Logger.getLogger(DebeziumRecordSource.class);

    private final String channel;
    private final OutboxStore store;
    private final OutboxRecordSourceContext sourceContext;
    private final Executor context;
    private final Duration leaseDuration;
    private final boolean keepDelivered;
    private final boolean unordered;
    private final ChangeEventDecoder decoder;
    private final EngineLauncher launcher;
    private final Properties engineProperties;

    // --- Relay context only ---
    private OutboxRecordSink sink;
    private Listener listener;
    private boolean running;
    private boolean leaseHeld;
    private boolean leaseRequestPending;
    /** Changes on stop and on lease loss. A batch or a callback of an older epoch is ignored. */
    private long epoch;
    private Closeable engine;
    private Cancellable leaseTimer;
    /** Records read from the log and not handed to the relay yet, in commit order. */
    private final Deque<OutboxRecord> backlog = new ArrayDeque<>();
    /** Under ordering=key: records of a key the relay is retrying, oldest first. */
    private final Map<String, Deque<OutboxRecord>> waiting = new LinkedHashMap<>();
    /**
     * The batches each unsettled record belongs to. Usually one: a record can appear twice when it is committed
     * while the snapshot runs, and both batches wait for it.
     */
    private final Map<UUID, List<Batch>> batches = new HashMap<>();

    /**
     * @param unordered {@code ordering=none}: records handed back are held until their retry is due
     */
    public DebeziumRecordSource(OutboxRecordSourceContext sourceContext, Duration leaseDuration, boolean keepDelivered,
            boolean unordered, EngineLauncher launcher, Properties engineProperties) {
        this.channel = sourceContext.getChannel();
        this.store = sourceContext.getStore();
        this.sourceContext = sourceContext;
        this.context = sourceContext.getExecutor();
        this.leaseDuration = leaseDuration;
        this.keepDelivered = keepDelivered;
        this.unordered = unordered;
        this.decoder = new ChangeEventDecoder(channel);
        this.launcher = launcher;
        this.engineProperties = engineProperties;
    }

    /** The records of one engine batch that the relay has not finished with yet. */
    private static final class Batch {
        int remaining;
        final CompletableFuture<Void> settled = new CompletableFuture<>();

        Batch(int size) {
            this.remaining = size;
        }
    }

    // --- Lifecycle ---

    @Override
    public void start(OutboxRecordSink sink, Listener listener) {
        this.sink = sink;
        this.listener = listener;
        this.running = true;
        this.epoch++;
        this.leaseHeld = false;
        this.leaseRequestPending = false;
        leaseTimer = sourceContext.schedulePeriodically(Duration.ofMillis(Math.max(1, leaseDuration.toMillis() / 3)),
                this::holdLease);
        holdLease();
    }

    @Override
    public void stop() {
        running = false;
        if (leaseTimer != null) {
            leaseTimer.cancel();
            leaseTimer = null;
        }
        leaseHeld = false;
        abandon();
    }

    @Override
    public Uni<Void> close(List<OutboxRecord> unsent) {
        // Unsent records stay pending in the table: the next reader's snapshot finds them.
        return Uni.createFrom().<Boolean> deferred(() -> store.holdLease(Duration.ZERO))
                .onFailure().invoke(e -> log.warnf(e, "Failed to give up the lease on channel '%s'", channel))
                .onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    // --- Lease ---

    private void holdLease() {
        if (!running || leaseRequestPending) {
            return;
        }
        boolean acquire = !leaseHeld;
        leaseRequestPending = true;
        Uni.createFrom().<Boolean> deferred(() -> store.holdLease(leaseDuration, acquire))
                .emitOn(context)
                .subscribe().with(held -> {
                    leaseRequestPending = false;
                    if (!running) {
                        return;
                    }
                    if (!held) {
                        leaseLost();
                    } else if (acquire && !leaseHeld) {
                        log.infof("Outbox lease acquired for channel '%s', starting the Debezium engine", channel);
                        leaseHeld = true;
                        launchEngine();
                    }
                }, failure -> {
                    leaseRequestPending = false;
                    log.warnf(failure, "Outbox lease renewal failed for channel '%s'", channel);
                    leaseLost();
                });
    }

    private void leaseLost() {
        if (!leaseHeld) {
            return;
        }
        log.warnf("Outbox lease lost for channel '%s', stopping the Debezium engine", channel);
        leaseHeld = false;
        abandon();
        listener.lostAccess();
    }

    // --- Engine ---

    private void launchEngine() {
        long launchEpoch = epoch;
        engine = launcher.launch(engineProperties, values -> onBatch(values, launchEpoch),
                failure -> context.execute(() -> {
                    if (running && launchEpoch == epoch) {
                        listener.failed(failure);
                    }
                }));
    }

    /** Stops the engine and abandons the batches in progress: their position is not recorded. */
    private void abandon() {
        epoch++;
        backlog.clear();
        waiting.clear();
        for (List<Batch> waitingBatches : batches.values()) {
            waitingBatches.forEach(b -> b.settled.cancel(false));
        }
        batches.clear();
        Closeable running = engine;
        engine = null;
        if (running != null) {
            // Closing waits for the engine thread, which may be leaving onBatch: not on the relay's context.
            CompletableFuture.runAsync(() -> {
                try {
                    running.close();
                } catch (Exception e) {
                    log.warnf(e, "Could not stop the Debezium engine of channel '%s'", channel);
                }
            });
        }
    }

    /** On the engine thread: hands the batch's records to the relay and waits until all are settled. */
    void onBatch(List<String> values, long launchEpoch) throws InterruptedException {
        List<OutboxRecord> records = new ArrayList<>();
        for (String value : values) {
            try {
                decoder.decode(value).ifPresent(records::add);
            } catch (RuntimeException e) {
                // The row is still pending in the table: the snapshot at the next start reads it again.
                log.errorf(e, "Outbox could not read a change event on channel '%s', skipping it: %s", channel, value);
            }
        }
        if (records.isEmpty()) {
            return;
        }
        Batch batch = new Batch(records.size());
        context.execute(() -> offer(records, batch, launchEpoch));
        try {
            batch.settled.get();
        } catch (CancellationException | ExecutionException e) {
            throw new InterruptedException("Outbox batch abandoned on channel '" + channel + "'");
        }
    }

    private void offer(List<OutboxRecord> records, Batch batch, long launchEpoch) {
        if (launchEpoch != epoch) {
            batch.settled.cancel(false);
            return;
        }
        for (OutboxRecord record : records) {
            List<Batch> owners = batches.get(record.getId());
            if (owners != null) {
                owners.add(batch); // already on its way: this batch waits for the same outcome
                continue;
            }
            owners = new ArrayList<>(1);
            owners.add(batch);
            batches.put(record.getId(), owners);
            backlog.add(record);
        }
        drain();
    }

    /** Hands records to the relay while it has room, keeping back those of keys it is retrying. */
    private void drain() {
        if (!running || !leaseHeld) {
            return;
        }
        if (!waiting.isEmpty()) {
            Set<String> busy = sink.busyGroups();
            Iterator<Map.Entry<String, Deque<OutboxRecord>>> keys = waiting.entrySet().iterator();
            while (keys.hasNext() && sink.remainingCapacity() > 0) {
                Map.Entry<String, Deque<OutboxRecord>> entry = keys.next();
                if (busy.contains(entry.getKey())) {
                    continue;
                }
                Deque<OutboxRecord> queue = entry.getValue();
                while (!queue.isEmpty() && sink.remainingCapacity() > 0) {
                    sink.accept(List.of(queue.poll()));
                }
                if (queue.isEmpty()) {
                    keys.remove();
                }
            }
        }
        while (!backlog.isEmpty() && sink.remainingCapacity() > 0) {
            OutboxRecord record = backlog.poll();
            Deque<OutboxRecord> queue = record.getMsgKey() == null ? null : waiting.get(record.getMsgKey());
            if (queue != null) {
                queue.add(record); // behind the records of its key the relay handed back
            } else {
                sink.accept(List.of(record));
            }
        }
    }

    private void settle(OutboxRecord record) {
        List<Batch> owners = batches.remove(record.getId());
        if (owners != null) {
            for (Batch batch : owners) {
                if (--batch.remaining == 0) {
                    batch.settled.complete(null);
                }
            }
        }
        drain();
    }

    // --- OutboxRecordSource ---

    @Override
    public void capacityAvailable() {
        drain();
    }

    @Override
    public Uni<Void> delivered(OutboxRecord record) {
        // The broker has the message: the record is settled even if recording it fails. The row then stays
        // pending and is sent again after a restart, a duplicate rather than a loss.
        return recorded(() -> store.delivered(record, keepDelivered))
                .onTermination().invoke(() -> settle(record));
    }

    @Override
    public Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        // Nothing is written: an update to a pending row would come back from the log as a record to send.
        // The relay keeps the attempt count in memory.
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> parked(OutboxRecord record, Throwable failure) {
        // Settled only once the park is recorded. If it fails, the batch is not finished, and the record is read
        // again after a restart instead of being lost.
        return recorded(() -> store.park(record, failure))
                .invoke(() -> settle(record));
    }

    @Override
    public void released(List<OutboxRecord> records) {
        if (unordered) {
            // A nacked record: hand it out again when its retry is due.
            long releaseEpoch = epoch;
            for (OutboxRecord record : records) {
                Instant due = record.getNextAttemptAt();
                long delayMs = due == null ? 0
                        : Math.max(0, Duration.between(sourceContext.getClock().instant(), due).toMillis());
                sourceContext.schedule(Duration.ofMillis(delayMs), () -> {
                    if (releaseEpoch == epoch && batches.containsKey(record.getId())) {
                        backlog.add(record);
                        drain();
                    }
                });
            }
            return;
        }
        // Under ordering=key, the records behind a nacked one. They go back before anything later of their key.
        for (int i = records.size() - 1; i >= 0; i--) {
            OutboxRecord record = records.get(i);
            if (!batches.containsKey(record.getId())) {
                continue; // abandoned meanwhile
            }
            if (record.getMsgKey() == null) {
                backlog.addFirst(record);
            } else {
                waiting.computeIfAbsent(record.getMsgKey(), k -> new ArrayDeque<>()).addFirst(record);
            }
        }
    }

    private Uni<Void> recorded(Supplier<Uni<Void>> write) {
        return Uni.createFrom().<Void> deferred(write::get)
                .emitOn(context)
                .onFailure(LeaseLostException.class).invoke(this::leaseLost);
    }
}
