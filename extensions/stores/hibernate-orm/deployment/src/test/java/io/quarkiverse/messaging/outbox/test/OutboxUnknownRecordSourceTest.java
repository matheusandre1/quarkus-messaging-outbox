package io.quarkiverse.messaging.outbox.test;

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxUnknownRecordSourceTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class, SimpleStringMapper.class))
            .overrideConfigKey("mp.messaging.outgoing.test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.test-channel.outbox.relay.source", "nonexistent-source")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql")
            .assertException(t -> {
                String msg = rootMessage(t);
                if (!msg.contains("nonexistent-source")) {
                    fail("Expected error mentioning 'nonexistent-source', got: " + msg);
                }
            });

    @Test
    void unknownRecordSourceFailsStartup() {
        fail("Should not reach here: startup should have failed");
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t.getMessage() != null ? t.getMessage() : t.toString();
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("test-channel")
        OutboxEmitter<String> emitter;
    }
}
