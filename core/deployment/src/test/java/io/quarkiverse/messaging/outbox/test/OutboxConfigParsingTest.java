package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;

public class OutboxConfigParsingTest {

    @Test
    void e5_defaultValues() {
        OutboxConfig config = OutboxConfig.defaults();
        assertThat(config.batchSize()).isEqualTo(100);
        assertThat(config.maxAttempts()).isEqualTo(-1);
        assertThat(config.backoffInitial()).isEqualTo(Duration.ofSeconds(1));
        assertThat(config.backoffMax()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(config.leaseDuration()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.ordering()).isEqualTo("strict");
        assertThat(config.retention()).isEqualTo("delete");
        assertThat(config.maxInFlight()).isEqualTo(1024);
    }

    static org.eclipse.microprofile.config.Config channel(Map<String, String> values) {
        return new SmallRyeConfigBuilder().withSources(new PropertiesConfigSource(values, "channel", 100)).build();
    }

    @Test
    void removeAfterInsertRequiresTheRelayToBeDisabled() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> OutboxConfig.from(channel(Map.of(
                "outbox.remove-after-insert", "true"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outbox.relay.enabled=false");

        var config = channel(Map.of("outbox.remove-after-insert", "true", "outbox.relay.enabled", "false"));
        assertThat(OutboxConfig.from(config).relayEnabled()).isFalse();
        assertThat(OutboxConfig.removeAfterInsert(config)).isTrue();
        assertThat(OutboxConfig.removeAfterInsert(channel(Map.of()))).isFalse();
    }

    @Test
    void e5_customValues() {
        OutboxConfig config = new OutboxConfig(50, 5, Duration.ofMillis(500), Duration.ofSeconds(30),
                Duration.ofSeconds(10), Duration.ofMinutes(1), "key", "keep", 512);
        assertThat(config.batchSize()).isEqualTo(50);
        assertThat(config.maxAttempts()).isEqualTo(5);
        assertThat(config.backoffInitial()).isEqualTo(Duration.ofMillis(500));
        assertThat(config.backoffMax()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.pollInterval()).isEqualTo(Duration.ofSeconds(10));
        assertThat(config.leaseDuration()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.ordering()).isEqualTo("key");
        assertThat(config.retention()).isEqualTo("keep");
        assertThat(config.maxInFlight()).isEqualTo(512);
    }
}
