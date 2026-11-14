package io.quarkiverse.messaging.outbox.spi;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import io.smallrye.mutiny.Uni;

/**
 * Persistence operations for the outbox table. Implementations are created by a {@link Factory} bean,
 * qualified with {@code @Identifier} (e.g., {@code "hibernate-orm"} or {@code "hibernate-reactive"}).
 * <p>
 * {@link #persist} runs in the caller's transaction. All other methods run in short transactions of their own,
 * on a worker thread (Hibernate ORM) or a dedicated Vert.x context (Hibernate Reactive).
 */
public interface OutboxStore {

    /**
     * Writes the record in the caller's transaction. {@code afterCommit} is called once the transaction
     * commits, to signal the relay. The returned Uni completes when the record is written, not when it
     * is delivered.
     */
    Uni<Void> persist(OutboxRecord record, Runnable afterCommit);

    /**
     * Claims up to {@link ClaimRequest#max()} pending records, oldest first, using
     * {@code SELECT ... FOR UPDATE SKIP LOCKED}. Claimed records have their {@code claimedBy} and
     * {@code claimedUntil} set.
     */
    Uni<List<OutboxRecord>> claim(ClaimRequest request);

    /** Marks a record as delivered: deletes it, or marks it as {@code SENT} when {@code keep} is true. */
    Uni<Void> delivered(OutboxRecord record, boolean keep);

    /** Records a failed delivery attempt: sets the next attempt time and the error message. */
    Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure);

    /** Parks a record as {@code FAILED} after too many attempts or a non-retryable error. */
    Uni<Void> park(OutboxRecord record, Throwable failure);

    /** Gives claimed records back so they can be claimed again. */
    Uni<Void> release(List<OutboxRecord> records);

    /** Extends {@code claimedUntil} on the given records so their claims don't expire while in flight. */
    Uni<Void> renewClaims(List<UUID> recordIds, Duration lease);

    /**
     * Acquires or renews the channel lease, or gives it up when {@code lease} is zero.
     * <p>
     * With {@code acquire}, the caller does not hold the lease yet. If it obtains it, the same transaction clears
     * the claims on the channel's pending records: records claimed by a previous holder, or by the caller before it
     * lost the lease, are then claimed again in order instead of being skipped until their claims expire.
     *
     * @return whether the caller holds the lease
     */
    Uni<Boolean> holdLease(Duration lease, boolean acquire);

    /** Renews the channel lease, or gives it up when {@code lease} is zero. */
    default Uni<Boolean> holdLease(Duration lease) {
        return holdLease(lease, false);
    }

    /** Returns the number of pending and failed records, and the age of the oldest pending record. */
    Uni<OutboxStats> stats();

    /** Deletes sent records older than {@code sentBefore}. Returns the number deleted. */
    Uni<Integer> purge(Instant sentBefore);

    /** Creates the store for one channel. Implementations are CDI beans qualified with {@code @Identifier}. */
    interface Factory {
        OutboxStore create(OutboxStoreContext context);
    }
}
