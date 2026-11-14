package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSink;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.quarkus.test.QuarkusExtensionTest;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.mutiny.Uni;

/**
 * A record source selected with outbox.relay.source replaces polling: its records reach the connector,
 * and their outcomes are reported back to it.
 */
public class OutboxCustomRecordSourceTest {

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar.addClasses(Sender.class, CollectorConnector.class, SimpleStringMapper.class,
                    FixedSourceFactory.class))
            .overrideConfigKey("mp.messaging.outgoing.custom-source.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.custom-source.outbox.relay.source", "fixed")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    @Any
    CollectorConnector connector;

    @Inject
    @Identifier("fixed")
    FixedSourceFactory factory;

    @Test
    void recordsComeFromTheSelectedSource() {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(connector.received()).hasSize(1);
            assertThat(factory.delivered()).containsExactly("from-fixed-source");
        });
        assertThat(factory.channel()).isEqualTo("custom-source");
        assertThat(new String((byte[]) connector.received().get(0).getPayload(), StandardCharsets.UTF_8))
                .isEqualTo("from-fixed-source");
    }

    @ApplicationScoped
    @Identifier("fixed")
    public static class FixedSourceFactory implements OutboxRecordSource.Factory {

        // Read through methods: the test injects a client proxy, and a field read on the proxy
        // would not reach the bean instance.
        private final List<String> delivered = new CopyOnWriteArrayList<>();
        private volatile String channel;

        public List<String> delivered() {
            return delivered;
        }

        public String channel() {
            return channel;
        }

        @Override
        public OutboxRecordSource create(OutboxRecordSourceContext context) {
            channel = context.getChannel();
            return new OutboxRecordSource() {

                @Override
                public void start(OutboxRecordSink sink, Listener listener) {
                    OutboxRecord record = new OutboxRecord();
                    record.setId(UUID.randomUUID());
                    record.setChannel(context.getChannel());
                    record.setPayload("from-fixed-source".getBytes(StandardCharsets.UTF_8));
                    record.setPayloadType("java.lang.String");
                    sink.accept(List.of(record));
                }

                @Override
                public void capacityAvailable() {
                }

                @Override
                public Uni<Void> delivered(OutboxRecord record) {
                    delivered.add(new String(record.getPayload(), StandardCharsets.UTF_8));
                    return Uni.createFrom().voidItem().emitOn(context.getExecutor());
                }

                @Override
                public Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure) {
                    return Uni.createFrom().voidItem().emitOn(context.getExecutor());
                }

                @Override
                public Uni<Void> parked(OutboxRecord record, Throwable failure) {
                    return Uni.createFrom().voidItem().emitOn(context.getExecutor());
                }

                @Override
                public void released(List<OutboxRecord> records) {
                }

                @Override
                public void stop() {
                }

                @Override
                public Uni<Void> close(List<OutboxRecord> unsent) {
                    return Uni.createFrom().voidItem();
                }
            };
        }
    }

    @ApplicationScoped
    public static class Sender {

        @Inject
        @Channel("custom-source")
        OutboxEmitter<String> emitter;
    }
}
