package io.quarkiverse.messaging.outbox.spi;

import java.time.Duration;
import java.util.Set;

/**
 * Parameters for a claim query.
 *
 * @param max the maximum number of records to claim
 * @param lease how long the claim stays valid without renewal
 * @param dueOnly if true, skip records whose {@code nextAttemptAt} is in the future (used by unordered channels)
 * @param excludedKeys keys to exclude from the claim (used by key ordering to skip keys already in flight)
 */
public record ClaimRequest(
        int max,
        Duration lease,
        boolean dueOnly,
        Set<String> excludedKeys) {
}
