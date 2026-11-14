package io.quarkiverse.messaging.outbox.runtime.relay;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;

/**
 * Everything that differs between the three ordering modes. The rest of the relay is the same for all of them.
 */
public enum OrderingPolicy {

    /** The whole channel is one group: records are sent one at a time, in the order they were claimed. */
    STRICT {
        @Override
        public String groupOf(OutboxRecord record) {
            return "";
        }

        @Override
        public int maxHeld(OutboxConfig config) {
            return config.batchSize();
        }
    },

    /** One group per key. Records without a key are not ordered. */
    KEY {
        @Override
        public String groupOf(OutboxRecord record) {
            return record.getMsgKey();
        }

        @Override
        public boolean excludeBusyGroupsFromClaims() {
            return true;
        }

        @Override
        public boolean releaseGroupTailOnRetry() {
            return true;
        }
    },

    /** No groups: records are sent as they are claimed, and several relays may work in parallel. */
    NONE {
        @Override
        public String groupOf(OutboxRecord record) {
            return null;
        }

        @Override
        public boolean needsLease() {
            return false;
        }

        @Override
        public boolean claimDueOnly() {
            return true;
        }
    };

    /** The group a record is sent in, or {@code null} when the record is not ordered. */
    public abstract String groupOf(OutboxRecord record);

    /** How many records the relay may hold in memory at once. */
    public int maxHeld(OutboxConfig config) {
        return config.maxInFlight();
    }

    /** Whether only one relay may work on the channel at a time. */
    public boolean needsLease() {
        return true;
    }

    /**
     * Whether a claim skips records that are waiting for a retry.
     * Ordered modes must not skip them, or the records behind would be sent first.
     */
    public boolean claimDueOnly() {
        return false;
    }

    /** Whether a claim leaves out the groups the relay is already working on. */
    public boolean excludeBusyGroupsFromClaims() {
        return false;
    }

    /** Whether the records queued behind a failed one are given back while it waits for its retry. */
    public boolean releaseGroupTailOnRetry() {
        return false;
    }

    public static OrderingPolicy of(String ordering) {
        if ("strict".equals(ordering)) {
            return STRICT;
        }
        if ("key".equals(ordering)) {
            return KEY;
        }
        return NONE;
    }
}
