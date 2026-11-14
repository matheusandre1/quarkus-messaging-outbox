package io.quarkiverse.messaging.outbox.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.LeaseLostException;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.smallrye.mutiny.Uni;

public class InMemoryOutboxStore implements OutboxStore {

    private final List<OutboxRecord> records;
    private final AtomicLong seqGen;
    private final Clock clock;
    private final String ownerId;
    private final SharedLeaseState leaseState;

    private boolean failOnClaim;
    private boolean leaseLost;
    int claimCount;
    int deliveredCount;
    int retryCount;
    int parkCount;
    int releaseCount;

    public InMemoryOutboxStore(Clock clock, String ownerId) {
        this(clock, ownerId, new ArrayList<>(), new AtomicLong(1), new SharedLeaseState());
    }

    public InMemoryOutboxStore(Clock clock, String ownerId, List<OutboxRecord> records,
            AtomicLong seqGen, SharedLeaseState leaseState) {
        this.clock = clock;
        this.ownerId = ownerId;
        this.records = records;
        this.seqGen = seqGen;
        this.leaseState = leaseState;
    }

    public void addPending(OutboxRecord record) {
        record.setSeq(seqGen.getAndIncrement());
        record.setStatus(OutboxRecord.STATUS_PENDING);
        record.setCreatedAt(clock.instant());
        if (record.getNextAttemptAt() == null) {
            record.setNextAttemptAt(clock.instant());
        }
        records.add(record);
    }

    @Override
    public Uni<Void> persist(OutboxRecord record, Runnable afterCommit) {
        addPending(record);
        afterCommit.run();
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<List<OutboxRecord>> claim(ClaimRequest request) {
        claimCount++;
        if (failOnClaim) {
            return Uni.createFrom().failure(new RuntimeException("claim failed"));
        }
        checkLease();

        Instant now = clock.instant();
        List<OutboxRecord> result = records.stream()
                .filter(r -> r.getStatus() == OutboxRecord.STATUS_PENDING)
                .filter(r -> r.getClaimedUntil() == null || r.getClaimedUntil().isBefore(now))
                .filter(r -> !request.dueOnly() || r.getNextAttemptAt() == null || !r.getNextAttemptAt().isAfter(now))
                .filter(r -> request.excludedKeys().isEmpty()
                        || r.getMsgKey() == null
                        || !request.excludedKeys().contains(r.getMsgKey()))
                .sorted(Comparator.comparingLong(OutboxRecord::getSeq))
                .limit(request.max())
                .toList();

        for (OutboxRecord r : result) {
            r.setClaimedBy(ownerId);
            r.setClaimedUntil(now.plus(request.lease()));
        }
        return Uni.createFrom().item(result);
    }

    @Override
    public Uni<Void> delivered(OutboxRecord record, boolean keep) {
        deliveredCount++;
        if (leaseLost) {
            return Uni.createFrom().failure(new LeaseLostException("lease lost for " + ownerId));
        }
        record.setStatus(OutboxRecord.STATUS_SENT);
        record.setClaimedBy(null);
        record.setClaimedUntil(null);
        records.remove(record);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> retry(OutboxRecord record, Instant nextAttempt, Throwable failure) {
        retryCount++;
        if (leaseLost) {
            return Uni.createFrom().failure(new LeaseLostException("lease lost for " + ownerId));
        }
        record.setNextAttemptAt(nextAttempt);
        record.setLastError(failure != null ? failure.getMessage() : null);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> park(OutboxRecord record, Throwable failure) {
        parkCount++;
        if (leaseLost) {
            return Uni.createFrom().failure(new LeaseLostException("lease lost for " + ownerId));
        }
        record.setStatus(OutboxRecord.STATUS_FAILED);
        record.setLastError(failure != null ? failure.getMessage() : null);
        record.setClaimedBy(null);
        record.setClaimedUntil(null);
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> release(List<OutboxRecord> toRelease) {
        releaseCount++;
        for (OutboxRecord r : toRelease) {
            r.setClaimedBy(null);
            r.setClaimedUntil(null);
        }
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> renewClaims(List<UUID> recordIds, Duration lease) {
        Instant until = clock.instant().plus(lease);
        for (OutboxRecord r : records) {
            if (recordIds.contains(r.getId()) && ownerId.equals(r.getClaimedBy())) {
                r.setClaimedUntil(until);
            }
        }
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Boolean> holdLease(Duration lease, boolean acquire) {
        if (lease.isZero()) {
            if (ownerId.equals(leaseState.owner)) {
                leaseState.owner = null;
                leaseState.expiry = null;
            }
            return Uni.createFrom().item(true);
        }
        Instant now = clock.instant();
        if (leaseState.owner == null || leaseState.owner.equals(ownerId)
                || (leaseState.expiry != null && leaseState.expiry.isBefore(now))) {
            leaseState.owner = ownerId;
            leaseState.expiry = now.plus(lease);
            if (acquire) {
                for (OutboxRecord r : records) {
                    if (r.getStatus() == OutboxRecord.STATUS_PENDING) {
                        r.setClaimedBy(null);
                        r.setClaimedUntil(null);
                    }
                }
            }
            return Uni.createFrom().item(true);
        }
        return Uni.createFrom().item(false);
    }

    @Override
    public Uni<OutboxStats> stats() {
        long pending = records.stream().filter(r -> r.getStatus() == OutboxRecord.STATUS_PENDING).count();
        long failed = records.stream().filter(r -> r.getStatus() == OutboxRecord.STATUS_FAILED).count();
        return Uni.createFrom().item(new OutboxStats(pending, failed, Duration.ZERO));
    }

    @Override
    public Uni<Integer> purge(Instant sentBefore) {
        return Uni.createFrom().item(0);
    }

    public void setFailOnClaim(boolean fail) {
        this.failOnClaim = fail;
    }

    public void setLeaseLost(boolean lost) {
        this.leaseLost = lost;
    }

    private void checkLease() {
        if (leaseLost) {
            throw new LeaseLostException("lease lost for " + ownerId);
        }
    }

    public List<OutboxRecord> allRecords() {
        return List.copyOf(records);
    }

    public static class SharedLeaseState {
        String owner;
        Instant expiry;
    }
}
