package io.quarkiverse.messaging.outbox.runtime.relay;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;

/**
 * Gets records from the outbox table by claiming them from the {@link OutboxStore}.
 * <p>
 * It claims when it is signalled, on a poll timer, and whenever the dispatcher frees enough room.
 * For ordered channels it also holds the channel lease, so that only one relay reads the channel at a time.
 * <p>
 * Not thread-safe: every method runs on the relay's context.
 */
public final class PollingRecordSource implements OutboxRecordSource {

    private static final Logger log = Logger.getLogger(PollingRecordSource.class);

    private final OutboxStore store;
    private final OutboxConfig config;
    private final OrderingPolicy policy;
    private final Clock clock;
    private final String channel;
    private final Vertx vertx;
    private final Executor context;

    private OutboxRecordSink sink;
    private Listener listener;
    private boolean running;
    private boolean leaseHeld;
    private boolean claimPending;
    /** A lease request is in flight. Two acquisitions must not overlap: the second would clear the first's claims. */
    private boolean leaseRequestPending;
    /** The last claim came back full, so the table probably holds more records. */
    private boolean moreLikely;
    /** Changes on stop and on a lease loss. A claim that was running across such a change is discarded. */
    private long epoch;
    private long pollTimer = -1;
    private long leaseTimer = -1;
    private long purgeTimer = -1;

    public PollingRecordSource(OutboxStore store, OutboxConfig config, OrderingPolicy policy, Clock clock,
            String channel, Vertx vertx, Executor context) {
        this.store = store;
        this.config = config;
        this.policy = policy;
        this.clock = clock;
        this.channel = channel;
        this.vertx = vertx;
        this.context = context;
    }

    // --- Lifecycle ---

    @Override
    public void start(OutboxRecordSink sink, Listener listener) {
        this.sink = sink;
        this.listener = listener;
        this.running = true;
        this.epoch++;
        this.claimPending = false;
        this.moreLikely = false;

        long renewEvery = Math.max(1, config.leaseDuration().toMillis() / 3);
        if (policy.needsLease()) {
            leaseHeld = false;
            leaseTimer = every(renewEvery, this::holdLease);
            holdLease(); // the first claim follows as soon as the lease is acquired
        } else {
            leaseHeld = true;
            leaseTimer = every(renewEvery, this::renewClaims);
        }
        pollTimer = every(config.pollInterval().toMillis(), this::claim);
        if (config.isKeepRetention()) {
            purgeTimer = every(config.purgeInterval().toMillis(), this::purge);
        }
        claim();
    }

    /** A transaction that wrote a record has committed: claim now instead of waiting for the poll timer. */
    @Override
    public void signal() {
        claim();
    }

    /** Stops claiming. Outcomes of records already handed out can still be reported. */
    @Override
    public void stop() {
        running = false;
        epoch++;
        pollTimer = cancel(pollTimer);
        leaseTimer = cancel(leaseTimer);
        purgeTimer = cancel(purgeTimer);
    }

    /** Gives the records back and gives up the channel lease. Never fails. */
    @Override
    public Uni<Void> close(List<OutboxRecord> records) {
        Uni<Void> release = records.isEmpty()
                ? Uni.createFrom().voidItem()
                : Uni.createFrom().<Void> deferred(() -> store.release(records))
                        .onFailure().invoke(e -> log.warnf(e, "Failed to release records on channel '%s'", channel))
                        .onFailure().recoverWithNull();
        return release
                .chain(() -> Uni.createFrom().<Boolean> deferred(() -> store.holdLease(Duration.ZERO))
                        .onFailure().invoke(e -> log.warnf(e, "Failed to give up the lease on channel '%s'", channel))
                        .onFailure().recoverWithNull())
                .replaceWithVoid();
    }

    private long every(long periodMs, Runnable action) {
        return vertx.setPeriodic(periodMs, id -> context.execute(action));
    }

    private long cancel(long timerId) {
        if (timerId >= 0) {
            vertx.cancelTimer(timerId);
        }
        return -1;
    }

    // --- Claiming ---

    private void claim() {
        if (!running || !leaseHeld || claimPending) {
            return;
        }
        int room = sink.remainingCapacity();
        if (room <= 0) {
            return;
        }
        int max = Math.min(config.batchSize(), room);
        Set<String> excludedGroups = policy.excludeBusyGroupsFromClaims() ? sink.busyGroups() : Set.of();
        ClaimRequest request = new ClaimRequest(max, config.leaseDuration(), policy.claimDueOnly(), excludedGroups);
        long claimEpoch = epoch;
        claimPending = true;
        Uni.createFrom().<List<OutboxRecord>> deferred(() -> store.claim(request))
                .emitOn(context)
                .subscribe().with(
                        records -> {
                            if (claimEpoch != epoch) {
                                released(records); // stopped or lost the lease meanwhile
                                return;
                            }
                            claimPending = false;
                            moreLikely = records.size() >= max;
                            sink.accept(records);
                            capacityAvailable();
                        },
                        failure -> {
                            if (claimEpoch != epoch) {
                                return;
                            }
                            claimPending = false;
                            moreLikely = false;
                            if (failure instanceof LeaseLostException) {
                                leaseLost();
                            } else {
                                log.warnf(failure, "Outbox claim failed on channel '%s'", channel);
                            }
                        });
    }

    /** Claims again without waiting for the poll timer, once enough room is free. */
    @Override
    public void capacityAvailable() {
        if (!moreLikely) {
            return;
        }
        int threshold = Math.max(1, Math.min(config.batchSize(), policy.maxHeld(config)) / 2);
        if (sink.remainingCapacity() >= threshold) {
            claim();
        }
    }

    /** Keeps the claims of the held records alive while they wait or are in flight. */
    private void renewClaims() {
        if (!running) {
            return;
        }
        List<UUID> ids = sink.heldIds();
        if (ids.isEmpty()) {
            return;
        }
        Uni.createFrom().<Void> deferred(() -> store.renewClaims(ids, config.leaseDuration()))
                .subscribe().with(v -> {
                }, e -> log.warnf(e, "Outbox claim renewal failed on channel '%s'", channel));
    }

    // --- Lease ---

    private void holdLease() {
        if (!running || leaseRequestPending) {
            return;
        }
        renewClaims();
        // Acquiring clears the channel's claims in the same transaction, so no claim may run before it commits:
        // claim() waits for leaseHeld, which is set below.
        boolean acquire = !leaseHeld;
        leaseRequestPending = true;
        Uni.createFrom().<Boolean> deferred(() -> store.holdLease(config.leaseDuration(), acquire))
                .emitOn(context)
                .subscribe().with(
                        held -> {
                            leaseRequestPending = false;
                            if (!running) {
                                return;
                            }
                            if (!held) {
                                leaseLost();
                            } else if (acquire && !leaseHeld) {
                                // A renewal that succeeds after the lease was lost meanwhile does not count: the claims
                                // were not cleared. The next request acquires it properly.
                                log.infof("Outbox lease acquired for channel '%s'", channel);
                                leaseHeld = true;
                                claim();
                            }
                        },
                        failure -> {
                            leaseRequestPending = false;
                            log.warnf(failure, "Outbox lease renewal failed for channel '%s'", channel);
                            leaseLost();
                        });
    }

    private void leaseLost() {
        if (!leaseHeld || !policy.needsLease()) {
            return;
        }
        log.warnf("Outbox lease lost for channel '%s', another relay took over", channel);
        leaseHeld = false;
        epoch++;
        claimPending = false;
        moreLikely = false;
        listener.lostAccess();
    }

    // --- OutboxRecordSource: outcomes reported by the dispatcher ---

    @Override
    public Uni<Void> delivered(OutboxRecord record) {
        return recorded(() -> store.delivered(record, config.isKeepRetention()));
    }

    @Override
    public Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        return recorded(() -> store.retry(record, nextAttempt, failure));
    }

    @Override
    public Uni<Void> parked(OutboxRecord record, Throwable failure) {
        return recorded(() -> store.park(record, failure));
    }

    @Override
    public void released(List<OutboxRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        Uni.createFrom().<Void> deferred(() -> store.release(records))
                .subscribe().with(v -> {
                }, e -> log.debugf(e, "Outbox could not release %d records on channel '%s'", records.size(), channel));
    }

    /**
     * A store write whose result is delivered on the relay's context. A write rejected because the lease
     * moved to another relay is turned into a lease loss before the caller sees the failure.
     */
    private Uni<Void> recorded(Supplier<Uni<Void>> write) {
        return Uni.createFrom().<Void> deferred(write::get)
                .emitOn(context)
                .onFailure(LeaseLostException.class).invoke(this::onLeaseLostFailure);
    }

    private void onLeaseLostFailure(Throwable failure) {
        leaseLost();
    }

    // --- Purge ---

    private void purge() {
        if (!running) {
            return;
        }
        Instant cutoff = clock.instant().minus(config.retentionPeriod());
        Uni.createFrom().<Integer> deferred(() -> store.purge(cutoff))
                .subscribe().with(
                        count -> {
                            if (count > 0) {
                                log.debugf("Outbox purged %d sent records on channel '%s'", count, channel);
                            }
                        },
                        e -> log.warnf(e, "Outbox purge failed on channel '%s'", channel));
    }
}
