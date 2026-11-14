package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.runtime.OutboxChannelTypes;
import io.quarkus.test.QuarkusExtensionTest;

public class OutboxPayloadTypeTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CustomerCreated.class, CollectorConnector.class))
            .overrideConfigKey("mp.messaging.outgoing.customer-events.connector", "test-collector");

    @Inject
    OutboxChannelTypes channelTypes;

    @Test
    void payloadTypeIsCapturedAtBuildTime() {
        Class<?> type = channelTypes.getPayloadType("customer-events");
        assertThat(type).isEqualTo(CustomerCreated.class);
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("customer-events")
        OutboxEmitter<CustomerCreated> emitter;
    }
}
