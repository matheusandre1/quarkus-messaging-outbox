package io.quarkiverse.messaging.outbox.runtime.relay;

import java.lang.reflect.Type;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.subscription.MultiEmitter;

/**
 * Sends records downstream and reports each outcome to the {@link OutboxRecordSource}.
 * <p>
 * Ordered records go through a {@link KeyGroup}, which sends one at a time and retries a failed one in place.
 * Unordered records are sent as they arrive. The {@link OrderingPolicy} decides which is which.
 * <p>
 * Not thread-safe: every method runs on the relay's context. Ack and nack calls arrive on other threads and
 * are moved onto that context first.
 */
public final class Dispatcher implements OutboxRecordSink, KeyGroup.GroupListener {

    /** Runs an action on the relay's context after a delay. */
    public interface Timer {
        void after(long delayMs, Runnable action);
    }

    private static final Logger log = Logger.getLogger(Dispatcher.class);

    /** A record that keeps being nacked is logged as a warning again every this many attempts. */
    static final int WARN_EVERY_ATTEMPTS = 10;

    private final OrderingPolicy policy;
    private final OutboxConfig config;
    private final OutboxRecordMapper mapper;
    private final List<OutboxMetadataConverter> metadataConverters;
    private final Clock clock;
    private final String channel;
    private final Executor context;
    private final Timer timer;
    private final OutboxRecordSource source;

    private MultiEmitter<? super Message<?>> emitter;
    private boolean stopped = true;
    /**
     * Changes every time the held records are dropped. Timers and ack or nack callbacks remember the value
     * they were created with, and do nothing if it has changed since.
     */
    private long generation;
    /** Messages sent downstream whose ack or nack has not been fully handled yet. */
    private int awaitingAck;
    private Runnable whenIdle;
    /** Records accepted and not finished: queued in a group, waiting for a retry, or sent downstream. */
    private final Map<UUID, OutboxRecord> held = new HashMap<>();
    private final Map<String, KeyGroup> groups = new HashMap<>();

    public Dispatcher(OrderingPolicy policy, OutboxConfig config, OutboxRecordMapper mapper,
            List<OutboxMetadataConverter> metadataConverters, Clock clock, String channel, Executor context,
            Timer timer, OutboxRecordSource source) {
        this.policy = policy;
        this.config = config;
        this.mapper = mapper;
        this.metadataConverters = metadataConverters;
        this.clock = clock;
        this.channel = channel;
        this.context = context;
        this.timer = timer;
        this.source = source;
    }

    // --- Lifecycle ---

    /** Starts sending to a new downstream subscriber. */
    public void start(MultiEmitter<? super Message<?>> emitter) {
        dropAll();
        this.emitter = emitter;
        this.stopped = false;
    }

    /** Stops sending new records. Messages already sent can still be acked or nacked. */
    public void stop() {
        stopped = true;
    }

    /** Runs the action once no sent message is waiting for its ack or nack. */
    public void whenIdle(Runnable action) {
        if (awaitingAck == 0) {
            action.run();
        } else {
            whenIdle = action;
        }
    }

    /**
     * Forgets every held record and returns them. Pending retries and late acks or nacks for these records
     * are ignored from now on.
     */
    public List<OutboxRecord> dropAll() {
        generation++;
        List<OutboxRecord> dropped = new ArrayList<>(held.values());
        held.clear();
        groups.clear();
        awaitingAck = 0;
        runWhenIdle();
        return dropped;
    }

    // --- OutboxRecordSink: records coming in ---

    @Override
    public int remainingCapacity() {
        return policy.maxHeld(config) - held.size();
    }

    @Override
    public Set<String> busyGroups() {
        return Set.copyOf(groups.keySet());
    }

    @Override
    public List<UUID> heldIds() {
        return List.copyOf(held.keySet());
    }

    @Override
    public void accept(List<OutboxRecord> records) {
        Set<KeyGroup> touched = new LinkedHashSet<>();
        for (OutboxRecord record : records) {
            if (held.putIfAbsent(record.getId(), record) != null) {
                continue; // already here
            }
            String groupName = policy.groupOf(record);
            if (groupName == null) {
                emit(null, record);
            } else {
                KeyGroup group = groups.computeIfAbsent(groupName, name -> new KeyGroup(name, clock, this::runAt, this));
                group.add(record);
                touched.add(group);
            }
        }
        touched.forEach(KeyGroup::start);
    }

    // --- Records going out ---

    /** Schedules a group's retry. It is skipped if the held records were dropped in the meantime. */
    private void runAt(Instant when, Runnable action) {
        long gen = generation;
        long delayMs = Math.max(1, Duration.between(clock.instant(), when).toMillis());
        timer.after(delayMs, () -> {
            if (gen == generation) {
                action.run();
            }
        });
    }

    /** Sends one record downstream. {@code group} is {@code null} for an unordered record. */
    @Override
    public void emit(KeyGroup group, OutboxRecord record) {
        if (stopped) {
            return; // the record stays held and is given back when the relay shuts down
        }
        Message<?> message = toMessage(record);
        if (message == null) {
            finished(group, record); // could not be mapped and was parked
            return;
        }
        awaitingAck++;
        emitter.emit(withAckAndNack(message, group, record));
    }

    @SuppressWarnings("unchecked")
    private Message<?> withAckAndNack(Message<?> message, KeyGroup group, OutboxRecord record) {
        long gen = generation;
        // A message is settled once: a second ack or nack on the same message is ignored.
        AtomicBoolean settled = new AtomicBoolean();
        return ((Message<Object>) message)
                .withAck(() -> onContextOnce(settled, done -> onAck(gen, group, record, done)))
                .withNack(error -> onContextOnce(settled, done -> onNack(gen, group, record, error, done)));
    }

    private CompletableFuture<Void> onContextOnce(AtomicBoolean settled, Consumer<CompletableFuture<Void>> action) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (settled.compareAndSet(false, true)) {
            context.execute(() -> action.accept(done));
        } else {
            done.complete(null);
        }
        return done;
    }

    // --- Outcomes ---

    private void onAck(long gen, KeyGroup group, OutboxRecord record, CompletableFuture<Void> done) {
        if (gen != generation) {
            // The record was dropped here, but the broker has the message: still try to remove it.
            source.delivered(record).emitOn(context).subscribe().with(v -> done.complete(null), e -> done.complete(null));
            return;
        }
        thenOnContext(gen, source.delivered(record), done, "record the delivery of", record,
                () -> finished(group, record));
    }

    private void onNack(long gen, KeyGroup group, OutboxRecord record, Throwable error, CompletableFuture<Void> done) {
        if (gen != generation) {
            done.complete(null);
            return;
        }
        record.setAttempts(record.getAttempts() + 1);
        if (config.maxAttempts() > 0 && record.getAttempts() >= config.maxAttempts()) {
            log.warnf(error, "Outbox parked record %s on channel '%s' after %d attempts. It will not be sent again"
                    + " unless its status is set back to pending.", record.getId(), channel, record.getAttempts());
            thenOnContext(gen, source.parked(record, error), done, "park", record,
                    () -> finished(group, record));
            return;
        }
        Instant nextAttempt = computeNextAttempt(record.getAttempts());
        logNack(record, error, nextAttempt);
        thenOnContext(gen, source.failed(record, nextAttempt, error), done, "record the failed attempt of", record,
                () -> retryLater(group, record, nextAttempt));
    }

    /**
     * A nack is logged as a warning on the first attempt and then every {@value #WARN_EVERY_ATTEMPTS} attempts, so a
     * record that keeps failing stays visible without a line per retry. The other attempts are logged at debug level.
     */
    private void logNack(OutboxRecord record, Throwable error, Instant nextAttempt) {
        int attempts = record.getAttempts();
        if (attempts == 1) {
            log.warnf(error, "Outbox record %s on channel '%s' was nacked, retrying at %s.%s",
                    record.getId(), channel, nextAttempt, blocking(record));
        } else if (attempts % WARN_EVERY_ATTEMPTS == 0) {
            log.warnf("Outbox record %s on channel '%s' was nacked %d times, retrying at %s.%s Last error: %s",
                    record.getId(), channel, attempts, nextAttempt, blocking(record), error);
        } else {
            log.debugf(error, "Outbox record %s on channel '%s' was nacked (attempt %d), retrying at %s",
                    record.getId(), channel, attempts, nextAttempt);
        }
    }

    /** What waits behind a failing record, as a sentence to append to a log message. */
    private String blocking(OutboxRecord record) {
        if (policy == OrderingPolicy.STRICT) {
            return " The records behind it on the channel wait.";
        }
        if (policy == OrderingPolicy.KEY && record.getMsgKey() != null) {
            return " The records with key '" + record.getMsgKey() + "' wait.";
        }
        return "";
    }

    /**
     * Runs {@code next} once the source has recorded an outcome, then completes the ack or nack.
     * A failure to record it is logged and does not stop the flow.
     */
    private void thenOnContext(long gen, Uni<Void> recorded, CompletableFuture<Void> done, String what,
            OutboxRecord record, Runnable next) {
        // The source's Uni may complete on any thread.
        recorded.emitOn(context).subscribe().with(
                v -> {
                    if (gen == generation) {
                        awaitingAck--;
                        next.run();
                    }
                    done.complete(null);
                },
                failure -> {
                    if (gen == generation) {
                        log.warnf(failure, "Outbox could not %s record %s on channel '%s'", what, record.getId(), channel);
                        awaitingAck--;
                        next.run();
                    }
                    done.complete(null);
                });
    }

    /** The record is finished: delivered or parked. */
    private void finished(KeyGroup group, OutboxRecord record) {
        held.remove(record.getId());
        if (group != null) {
            group.headSettled();
            if (group.isEmpty()) {
                groups.remove(group.name());
            }
        }
        runWhenIdle();
        source.capacityAvailable();
    }

    private void retryLater(KeyGroup group, OutboxRecord record, Instant nextAttempt) {
        record.setNextAttemptAt(nextAttempt);
        if (group == null) {
            // Unordered: give the record back, so that any relay picks it up when its retry is due.
            held.remove(record.getId());
            source.released(List.of(record));
            runWhenIdle();
            source.capacityAvailable();
            return;
        }
        if (policy.releaseGroupTailOnRetry()) {
            // Free the room the waiting group takes, so other groups keep flowing.
            List<OutboxRecord> tail = group.removeTail();
            tail.forEach(r -> held.remove(r.getId()));
            source.released(tail);
        }
        runWhenIdle();
        group.headRetrying();
    }

    private void runWhenIdle() {
        if (awaitingAck == 0 && whenIdle != null) {
            Runnable action = whenIdle;
            whenIdle = null;
            action.run();
        }
    }

    // --- Mapping ---

    /** Maps a record to a message. A record that cannot be mapped is parked, and {@code null} is returned. */
    private Message<?> toMessage(OutboxRecord record) {
        try {
            Message<?> message = mapper.toMessage(record, resolveType(record));
            return addOutboxMetadata(message, record);
        } catch (Exception e) {
            log.warnf(e, "Outbox mapper failed for record %s, parking", record.getId());
            source.parked(record, e).subscribe().with(v -> {
            }, failure -> log.errorf(failure, "Failed to park record %s", record.getId()));
            return null;
        }
    }

    private Type resolveType(OutboxRecord record) {
        String typeName = record.getPayloadType();
        if (typeName == null) {
            return null;
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = Dispatcher.class.getClassLoader();
        }
        try {
            return Class.forName(typeName, false, loader);
        } catch (ClassNotFoundException e) {
            // Sending raw bytes in place of the object would silently change the wire format.
            throw new IllegalStateException(
                    "Payload type " + typeName + " of outbox record " + record.getId() + " cannot be loaded", e);
        }
    }

    @SuppressWarnings({ "rawtypes" })
    private Message<?> addOutboxMetadata(Message<?> message, OutboxRecord record) {
        message = ((Message) message)
                .addMetadata(OutboxMetadata.of(record.getId(), record.getMsgKey(), record.getAttempts()));
        for (OutboxMetadataConverter converter : metadataConverters) {
            Metadata restored = converter.restore(record);
            for (Object m : restored) {
                message = ((Message) message).addMetadata(m);
            }
        }
        return message;
    }

    /** Exponential backoff with up to 20% jitter, capped at {@code backoff.max}. */
    public Instant computeNextAttempt(int attempts) {
        long initialMs = config.backoffInitial().toMillis();
        long maxMs = config.backoffMax().toMillis();
        long delayMs = (long) Math.min(maxMs, initialMs * Math.pow(2, attempts - 1));
        long jitter = (long) (delayMs * 0.2 * ThreadLocalRandom.current().nextDouble());
        return clock.instant().plus(Duration.ofMillis(delayMs + jitter));
    }
}
