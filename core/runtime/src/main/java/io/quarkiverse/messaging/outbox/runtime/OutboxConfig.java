package io.quarkiverse.messaging.outbox.runtime;

import java.time.Duration;
import java.util.Optional;

import org.eclipse.microprofile.config.Config;

public class OutboxConfig {

    private final String store;
    private final String keyExtractor;
    private final int batchSize;
    private final int maxAttempts;
    private final Duration backoffInitial;
    private final Duration backoffMax;
    private final Duration pollInterval;
    private final Duration leaseDuration;
    private final String ordering;
    private final String retention;
    private final int maxInFlight;
    private final boolean relayEnabled;
    private final Duration retentionPeriod;
    private final Duration purgeInterval;
    private final String mode;
    private final Duration fallbackTimeout;

    public OutboxConfig(int batchSize, int maxAttempts, Duration backoffInitial, Duration backoffMax,
            Duration pollInterval, Duration leaseDuration, String ordering, String retention, int maxInFlight) {
        this(null, batchSize, maxAttempts, backoffInitial, backoffMax, pollInterval, leaseDuration, ordering, retention,
                maxInFlight, true);
    }

    public OutboxConfig(int batchSize, int maxAttempts, Duration backoffInitial, Duration backoffMax,
            Duration pollInterval, Duration leaseDuration, String ordering, String retention, int maxInFlight,
            boolean relayEnabled) {
        this(null, batchSize, maxAttempts, backoffInitial, backoffMax, pollInterval, leaseDuration, ordering, retention,
                maxInFlight, relayEnabled);
    }

    public OutboxConfig(String store, int batchSize, int maxAttempts, Duration backoffInitial, Duration backoffMax,
            Duration pollInterval, Duration leaseDuration, String ordering, String retention, int maxInFlight,
            boolean relayEnabled) {
        this(store, null, batchSize, maxAttempts, backoffInitial, backoffMax, pollInterval, leaseDuration, ordering,
                retention, maxInFlight, relayEnabled);
    }

    public OutboxConfig(String store, String keyExtractor, int batchSize, int maxAttempts, Duration backoffInitial,
            Duration backoffMax, Duration pollInterval, Duration leaseDuration, String ordering, String retention,
            int maxInFlight, boolean relayEnabled) {
        this(store, keyExtractor, batchSize, maxAttempts, backoffInitial, backoffMax, pollInterval, leaseDuration,
                ordering, retention, maxInFlight, relayEnabled, Duration.ofDays(7), Duration.ofHours(1));
    }

    public OutboxConfig(String store, String keyExtractor, int batchSize, int maxAttempts, Duration backoffInitial,
            Duration backoffMax, Duration pollInterval, Duration leaseDuration, String ordering, String retention,
            int maxInFlight, boolean relayEnabled, Duration retentionPeriod, Duration purgeInterval) {
        this(store, keyExtractor, batchSize, maxAttempts, backoffInitial, backoffMax, pollInterval, leaseDuration,
                ordering, retention, maxInFlight, relayEnabled, retentionPeriod, purgeInterval,
                "transactional", Duration.ofSeconds(5));
    }

    public OutboxConfig(String store, String keyExtractor, int batchSize, int maxAttempts, Duration backoffInitial,
            Duration backoffMax, Duration pollInterval, Duration leaseDuration, String ordering, String retention,
            int maxInFlight, boolean relayEnabled, Duration retentionPeriod, Duration purgeInterval,
            String mode, Duration fallbackTimeout) {
        this.store = store;
        this.keyExtractor = keyExtractor;
        this.batchSize = batchSize;
        this.maxAttempts = maxAttempts;
        this.backoffInitial = backoffInitial;
        this.backoffMax = backoffMax;
        this.pollInterval = pollInterval;
        this.leaseDuration = leaseDuration;
        this.ordering = ordering;
        this.retention = retention;
        this.maxInFlight = maxInFlight;
        this.relayEnabled = relayEnabled;
        this.retentionPeriod = retentionPeriod;
        this.purgeInterval = purgeInterval;
        this.mode = mode;
        this.fallbackTimeout = fallbackTimeout;
    }

    public static OutboxConfig defaults() {
        return new OutboxConfig(null, null, 100, -1, Duration.ofSeconds(1), Duration.ofMinutes(1),
                Duration.ofSeconds(5), Duration.ofSeconds(30), "strict", "delete", 1024, true,
                Duration.ofDays(7), Duration.ofHours(1));
    }

    public static OutboxConfig from(Config config) {
        if (config == null) {
            return defaults();
        }
        String mode = config.getOptionalValue("outbox.mode", String.class).orElse("transactional");
        requireOneOf("outbox.mode", mode, "transactional", "fallback");
        boolean fallback = "fallback".equals(mode);
        // Fallback mode cannot keep order: stored records are sent after later direct sends.
        String ordering = config.getOptionalValue("outbox.ordering", String.class).orElse(fallback ? "none" : "strict");
        requireOneOf("outbox.ordering", ordering, "strict", "key", "none");
        if (fallback && !"none".equals(ordering)) {
            throw new IllegalArgumentException(
                    "outbox.mode=fallback requires outbox.ordering=none, but it is set to '" + ordering + "'");
        }
        requireOneOf("outbox.retention.mode", config.getOptionalValue("outbox.retention.mode", String.class).orElse("delete"),
                "delete", "keep");
        boolean relayEnabled = config.getOptionalValue("outbox.relay.enabled", Boolean.class).orElse(true);
        if (removeAfterInsert(config) && relayEnabled) {
            // The relay needs the row: polling claims it, and the Debezium source keeps its progress in it.
            throw new IllegalArgumentException("outbox.remove-after-insert=true requires outbox.relay.enabled=false:"
                    + " the record is deleted in the transaction that writes it, so only an external change data"
                    + " capture relay, reading the transaction log, can deliver it");
        }
        return new OutboxConfig(
                config.getOptionalValue("outbox.store", String.class).orElse(null),
                config.getOptionalValue("outbox.key-extractor", String.class).orElse(null),
                config.getOptionalValue("outbox.batch-size", Integer.class).orElse(100),
                config.getOptionalValue("outbox.max-attempts", Integer.class).orElse(-1),
                config.getOptionalValue("outbox.backoff.initial", Duration.class).orElse(Duration.ofSeconds(1)),
                config.getOptionalValue("outbox.backoff.max", Duration.class).orElse(Duration.ofMinutes(1)),
                config.getOptionalValue("outbox.poll-interval", Duration.class).orElse(Duration.ofSeconds(5)),
                config.getOptionalValue("outbox.lease-duration", Duration.class).orElse(Duration.ofSeconds(30)),
                ordering,
                config.getOptionalValue("outbox.retention.mode", String.class).orElse("delete"),
                config.getOptionalValue("outbox.max-in-flight", Integer.class).orElse(1024),
                config.getOptionalValue("outbox.relay.enabled", Boolean.class).orElse(true),
                config.getOptionalValue("outbox.retention.period", Duration.class).orElse(Duration.ofDays(7)),
                config.getOptionalValue("outbox.retention.purge-interval", Duration.class).orElse(Duration.ofHours(1)),
                mode,
                config.getOptionalValue("outbox.fallback.timeout", Duration.class).orElse(Duration.ofSeconds(5)));
    }

    /**
     * {@code outbox.remove-after-insert}: delete the record in the transaction that writes it. The insert stays in
     * the transaction log for an external change data capture relay, and the table stays empty.
     */
    public static boolean removeAfterInsert(Config config) {
        return config != null && config.getOptionalValue("outbox.remove-after-insert", Boolean.class).orElse(false);
    }

    private static void requireOneOf(String key, String value, String... allowed) {
        for (String candidate : allowed) {
            if (candidate.equals(value)) {
                return;
            }
        }
        throw new IllegalArgumentException(
                "Invalid value '" + value + "' for " + key + ". Allowed values: " + String.join(", ", allowed));
    }

    public Optional<String> store() {
        return Optional.ofNullable(store);
    }

    public Optional<String> keyExtractor() {
        return Optional.ofNullable(keyExtractor);
    }

    public int batchSize() {
        return batchSize;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration backoffInitial() {
        return backoffInitial;
    }

    public Duration backoffMax() {
        return backoffMax;
    }

    public Duration pollInterval() {
        return pollInterval;
    }

    public Duration leaseDuration() {
        return leaseDuration;
    }

    public String ordering() {
        return ordering;
    }

    public String retention() {
        return retention;
    }

    public int maxInFlight() {
        return maxInFlight;
    }

    public boolean relayEnabled() {
        return relayEnabled;
    }

    public Duration retentionPeriod() {
        return retentionPeriod;
    }

    public Duration purgeInterval() {
        return purgeInterval;
    }

    public boolean isKeepRetention() {
        return "keep".equals(retention);
    }

    public String mode() {
        return mode;
    }

    public Duration fallbackTimeout() {
        return fallbackTimeout;
    }

    public boolean isFallback() {
        return "fallback".equals(mode);
    }
}
