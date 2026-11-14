package io.quarkiverse.messaging.outbox.spi;

import java.time.Duration;

/**
 * A snapshot of the outbox table's state for one channel, used by the health check.
 *
 * @param pending the number of records in {@code PENDING} status
 * @param failed the number of records in {@code FAILED} (parked) status
 * @param oldestAge the age of the oldest pending record, or {@link Duration#ZERO} if none
 */
public record OutboxStats(
        long pending,
        long failed,
        Duration oldestAge) {
}
