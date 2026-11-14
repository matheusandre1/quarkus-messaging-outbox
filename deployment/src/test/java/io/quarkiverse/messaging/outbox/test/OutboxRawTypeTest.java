package io.quarkiverse.messaging.outbox.test;

import static org.junit.jupiter.api.Assertions.fail;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxRawTypeTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(BadSender.class))
            .setExpectedException(IllegalStateException.class);

    @Test
    void rawOutboxEmitterFailsTheBuild() {
        fail("Should not reach here — the build should have failed");
    }

    @ApplicationScoped
    @SuppressWarnings("rawtypes")
    public static class BadSender {

        @Inject
        @Channel("customer-events")
        OutboxEmitter emitter;
    }
}
