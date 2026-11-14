package io.quarkiverse.messaging.outbox.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;

class DebeziumSettingsTest {

    static SmallRyeConfig config(Map<String, String> values) {
        return new SmallRyeConfigBuilder().withSources(new PropertiesConfigSource(values, "test", 100)).build();
    }

    @Test
    void defaultsComeFromTheChannelAndTheDatasource() {
        Properties p = DebeziumSettings.of("order-events", config(Map.of()), config(Map.of(
                "quarkus.datasource.jdbc.url", "jdbc:postgresql://db.example:6543/shop?sslmode=disable",
                "quarkus.datasource.username", "app",
                "quarkus.datasource.password", "secret")));

        assertThat(p.getProperty("connector.class")).isEqualTo("io.debezium.connector.postgresql.PostgresConnector");
        assertThat(p.getProperty("slot.name")).isEqualTo("outbox_order_events");
        assertThat(p.getProperty("table.include.list")).isEqualTo("public.outbox_record");
        assertThat(p.getProperty("snapshot.select.statement.overrides.public.outbox_record"))
                .isEqualTo("SELECT * FROM public.outbox_record WHERE channel = 'order-events' AND status = 0 ORDER BY seq");
        assertThat(p.getProperty("database.hostname")).isEqualTo("db.example");
        assertThat(p.getProperty("database.port")).isEqualTo("6543");
        assertThat(p.getProperty("database.dbname")).isEqualTo("shop");
        assertThat(p.getProperty("database.user")).isEqualTo("app");
        assertThat(p.getProperty("database.password")).isEqualTo("secret");
    }

    @Test
    void mysqlDefaults() {
        Properties p = DebeziumSettings.of("orders", config(Map.of()), config(Map.of(
                "quarkus.datasource.db-kind", "mysql",
                "quarkus.datasource.jdbc.url", "jdbc:mysql://db.example/shop?useSSL=false",
                "quarkus.datasource.username", "app")));

        assertThat(p.getProperty("connector.class")).isEqualTo("io.debezium.connector.mysql.MySqlConnector");
        assertThat(p.getProperty("table.include.list")).isEqualTo("shop.outbox_record");
        assertThat(p.getProperty("database.include.list")).isEqualTo("shop");
        assertThat(p.getProperty("snapshot.select.statement.overrides.shop.outbox_record"))
                .isEqualTo("SELECT * FROM shop.outbox_record WHERE channel = 'orders' AND status = 0 ORDER BY seq");
        assertThat(p.getProperty("schema.history.internal"))
                .isEqualTo("io.debezium.relational.history.MemorySchemaHistory");
        assertThat(Long.parseLong(p.getProperty("database.server.id"))).isBetween(100_000L, 4_294_967_295L);
        assertThat(p.getProperty("database.hostname")).isEqualTo("db.example");
        assertThat(p.getProperty("database.port")).isEqualTo("3306");
        assertThat(p).doesNotContainKeys("slot.name", "plugin.name", "database.dbname");
    }

    @Test
    void anotherDatabaseNeedsItsConnectorSetExplicitly() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DebeziumSettings.of("orders", config(Map.of()),
                config(Map.of("quarkus.datasource.db-kind", "oracle"))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("PostgreSQL and MySQL");

        Properties p = DebeziumSettings.of("orders", config(Map.of(
                "outbox.relay.debezium.connector.class", "io.debezium.connector.oracle.OracleConnector",
                "outbox.relay.debezium.table.include.list", "APP.OUTBOX_RECORD")),
                config(Map.of("quarkus.datasource.db-kind", "oracle")));
        assertThat(p.getProperty("connector.class")).isEqualTo("io.debezium.connector.oracle.OracleConnector");
        assertThat(p.getProperty("table.include.list")).isEqualTo("APP.OUTBOX_RECORD");
    }

    @Test
    void channelPropertiesOverrideTheDefaults() {
        Properties p = DebeziumSettings.of("orders", config(Map.of(
                "outbox.relay.debezium.slot.name", "custom_slot",
                "outbox.relay.debezium.database.hostname", "replica")),
                config(Map.of("quarkus.datasource.reactive.url", "postgresql://primary:5432/shop",
                        "quarkus.messaging-outbox.table-name", "my_outbox")));

        assertThat(p.getProperty("slot.name")).isEqualTo("custom_slot");
        assertThat(p.getProperty("database.hostname")).isEqualTo("replica");
        assertThat(p.getProperty("database.dbname")).isEqualTo("shop");
        assertThat(p.getProperty("table.include.list")).isEqualTo("public.my_outbox");
    }
}
