package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogRecord;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.runtime.OutboxEmitterFactory;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxConfigVisibleTest {

    private static final List<LogRecord> logRecords = new ArrayList<>();

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.customer-events.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.customer-events.outbox.store", "probe")
            .setLogRecordPredicate(record -> true)
            .assertLogRecords(records -> logRecords.addAll(records));

    @Inject
    @Any
    OutboxEmitterFactory factory;

    @Test
    void outboxConfigKeysAreReadableFromChannelConfig() {
        Config channelConfig = factory.getChannelConfig("customer-events");
        assertThat(channelConfig).isNotNull();

        var storeValue = channelConfig.getOptionalValue("outbox.store", String.class);
        assertThat(storeValue).as("outbox.store key from channel config").hasValue("probe");
    }

    @Test
    void noUnknownPropertyWarningForOutboxKeys() {
        boolean hasOutboxWarning = logRecords.stream()
                .anyMatch(r -> r.getMessage() != null
                        && r.getMessage().contains("outbox")
                        && (r.getMessage().toLowerCase().contains("unknown")
                                || r.getMessage().toLowerCase().contains("unrecognized")));
        assertThat(hasOutboxWarning).as("no unknown property warning for outbox keys").isFalse();
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("customer-events")
        OutboxEmitter<String> emitter;
    }
}
