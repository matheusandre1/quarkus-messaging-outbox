package io.quarkiverse.messaging.outbox.debezium;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.microprofile.config.Config;

/**
 * The Debezium engine properties for one channel.
 * <p>
 * The defaults depend on {@code quarkus.datasource.db-kind}:
 * <ul>
 * <li>{@code postgresql}: the PostgreSQL connector with {@code pgoutput}, and a replication slot and a filtered
 * publication named after the channel.</li>
 * <li>{@code mysql}: the MySQL connector, reading the binlog as a replica with a random server id, and keeping the
 * table structures it has seen in memory.</li>
 * </ul>
 * For both:
 * <ul>
 * <li>Only the outbox record table is captured.</li>
 * <li>Offsets are kept in memory, with a snapshot of the channel's pending records, in {@code seq} order, at each
 * start. The outbox table is the durable progress: a record is deleted, or marked sent, once delivered. After a
 * restart, the snapshot finds what was not delivered, and streaming continues from there.</li>
 * <li>The connection is the default datasource's, from {@code quarkus.datasource.jdbc.url} or
 * {@code quarkus.datasource.reactive.url}, and its username and password.</li>
 * </ul>
 * Every {@code mp.messaging.outgoing.<channel>.outbox.relay.debezium.<property>} is passed to the engine as
 * {@code <property>}, and wins over the defaults.
 */
public final class DebeziumSettings {

    public static final String PREFIX = "outbox.relay.debezium.";

    static final String POSTGRES_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector";
    static final String MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector";

    private DebeziumSettings() {
    }

    public static Properties of(String channel, Config channelConfig, Config config) {
        String id = channel.replaceAll("[^A-Za-z0-9_]", "_").toLowerCase();
        String tableName = config.getOptionalValue("quarkus.messaging-outbox.table-name", String.class)
                .orElse("outbox_record");
        String dbKind = config.getOptionalValue("quarkus.datasource.db-kind", String.class).orElse("postgresql");

        Properties p = new Properties();
        p.setProperty("name", "outbox-" + id);
        p.setProperty("topic.prefix", "outbox-" + id); // required by Debezium, not used: nothing goes to Kafka
        p.setProperty("tombstones.on.delete", "false");
        // Keeps the log position moving when the outbox is idle but other tables are written.
        p.setProperty("heartbeat.interval.ms", "10000");
        p.setProperty("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore");
        p.setProperty("offset.flush.interval.ms", "1000");
        p.setProperty("snapshot.mode", "initial");

        Datasource ds = datasource(config);
        String table;
        switch (dbKind) {
            case "mysql" -> {
                // MySQL names tables by database.
                table = ds.dbname().orElse("outbox") + "." + tableName;
                p.setProperty("connector.class", MYSQL_CONNECTOR);
                ds.dbname().ifPresent(db -> p.setProperty("database.include.list", db));
                // The binlog client registers as a replica: its id must not clash with any other replica, including
                // another instance's engine after a takeover.
                p.setProperty("database.server.id",
                        String.valueOf(ThreadLocalRandom.current().nextLong(100_000L, 4_294_967_295L)));
                // Offsets are in memory, so the table structures it has seen are too.
                p.setProperty("schema.history.internal", "io.debezium.relational.history.MemorySchemaHistory");
                p.setProperty("include.schema.changes", "false");
            }
            case "postgresql" -> {
                table = "public." + tableName;
                p.setProperty("connector.class", POSTGRES_CONNECTOR);
                p.setProperty("plugin.name", "pgoutput");
                p.setProperty("slot.name", "outbox_" + id);
                p.setProperty("publication.name", "outbox_" + id);
                p.setProperty("publication.autocreate.mode", "filtered");
            }
            default -> {
                boolean connectorSet = channelConfig != null
                        && channelConfig.getOptionalValue(PREFIX + "connector.class", String.class).isPresent();
                if (!connectorSet) {
                    throw new IllegalStateException("The Debezium record source has defaults for PostgreSQL and MySQL,"
                            + " not '" + dbKind + "'. To try another database, set its connector with " + PREFIX
                            + "connector.class and its properties, including table.include.list, under " + PREFIX);
                }
                table = tableName; // untested: the channel's properties say what the connector needs
            }
        }
        p.setProperty("table.include.list", table);
        p.setProperty("snapshot.select.statement.overrides", table);
        p.setProperty("snapshot.select.statement.overrides." + table,
                "SELECT * FROM " + table + " WHERE channel = '" + channel.replace("'", "''")
                        + "' AND status = 0 ORDER BY seq");

        ds.host().ifPresent(h -> p.setProperty("database.hostname", h));
        p.setProperty("database.port", String.valueOf(ds.port().orElse("mysql".equals(dbKind) ? 3306 : 5432)));
        if ("postgresql".equals(dbKind)) {
            ds.dbname().ifPresent(db -> p.setProperty("database.dbname", db));
        }
        config.getOptionalValue("quarkus.datasource.username", String.class)
                .ifPresent(v -> p.setProperty("database.user", v));
        config.getOptionalValue("quarkus.datasource.password", String.class)
                .ifPresent(v -> p.setProperty("database.password", v));

        if (channelConfig != null) {
            for (String name : channelConfig.getPropertyNames()) {
                if (name.startsWith(PREFIX)) {
                    channelConfig.getOptionalValue(name, String.class)
                            .ifPresent(v -> p.setProperty(name.substring(PREFIX.length()), v));
                }
            }
        }
        return p;
    }

    private record Datasource(Optional<String> host, Optional<Integer> port, Optional<String> dbname) {
    }

    /** Host, port and database of jdbc:postgresql://..., jdbc:mysql://..., postgresql://..., mysql://... URLs. */
    private static Datasource datasource(Config config) {
        Optional<String> url = config.getOptionalValue("quarkus.datasource.jdbc.url", String.class)
                .or(() -> config.getOptionalValue("quarkus.datasource.reactive.url", String.class))
                .filter(u -> u.contains("//"));
        if (url.isEmpty()) {
            return new Datasource(Optional.empty(), Optional.empty(), Optional.empty());
        }
        String u = url.get();
        URI uri = URI.create("db:" + u.substring(u.indexOf("//")));
        String path = uri.getPath();
        return new Datasource(Optional.ofNullable(uri.getHost()),
                uri.getPort() > 0 ? Optional.of(uri.getPort()) : Optional.empty(),
                path != null && path.length() > 1 ? Optional.of(path.substring(1)) : Optional.empty());
    }
}
