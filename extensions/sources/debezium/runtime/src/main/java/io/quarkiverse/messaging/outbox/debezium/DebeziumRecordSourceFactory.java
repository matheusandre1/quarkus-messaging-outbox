package io.quarkiverse.messaging.outbox.debezium;

import java.util.Properties;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.ConfigProvider;

import io.quarkiverse.messaging.outbox.runtime.OutboxConfig;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.smallrye.common.annotation.Identifier;

/** Selected with {@code mp.messaging.outgoing.<channel>.outbox.relay.source=debezium}. */
@ApplicationScoped
@Identifier("debezium")
public class DebeziumRecordSourceFactory implements OutboxRecordSource.Factory {

    @Override
    public OutboxRecordSource create(OutboxRecordSourceContext context) {
        OutboxConfig config = OutboxConfig.from(context.getChannelConfig());
        Properties properties = DebeziumSettings.of(context.getChannel(), context.getChannelConfig(),
                ConfigProvider.getConfig());
        requireConnector(context.getChannel(), properties.getProperty("connector.class"));
        return new DebeziumRecordSource(context, config.leaseDuration(), config.isKeepRetention(),
                "none".equals(config.ordering()), new DebeziumEngineLauncher(), properties);
    }

    /** Fails at startup, naming the artifact to add, rather than when the engine starts. */
    private static void requireConnector(String channel, String connectorClass) {
        try {
            Class.forName(connectorClass, false, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            String artifact = switch (connectorClass) {
                case DebeziumSettings.POSTGRES_CONNECTOR -> "io.debezium:debezium-connector-postgres";
                case DebeziumSettings.MYSQL_CONNECTOR -> "io.debezium:debezium-connector-mysql";
                default -> "the Debezium connector of your database";
            };
            throw new IllegalStateException("The Debezium record source of channel '" + channel + "' needs "
                    + connectorClass + ". Add " + artifact + " to the application.", e);
        }
    }
}
