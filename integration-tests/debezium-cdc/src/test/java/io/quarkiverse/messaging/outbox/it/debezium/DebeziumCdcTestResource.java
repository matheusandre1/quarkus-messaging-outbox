package io.quarkiverse.messaging.outbox.it.debezium;

import java.util.Map;

import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;

import io.debezium.testing.testcontainers.ConnectorConfiguration;
import io.debezium.testing.testcontainers.DebeziumContainer;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import io.strimzi.test.container.StrimziKafkaCluster;

/**
 * Starts PostgreSQL (with logical replication), Kafka, and Kafka Connect (Debezium) on a shared Docker network.
 * The outbox relay is disabled: Debezium reads the outbox table from the WAL and publishes to Kafka.
 */
public class DebeziumCdcTestResource implements QuarkusTestResourceLifecycleManager {

    private Network network;
    private PostgreSQLContainer<?> postgres;
    private StrimziKafkaCluster kafka;
    private DebeziumContainer connect;

    @Override
    public Map<String, String> start() {
        network = Network.newNetwork();

        kafka = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withContainerCustomizer(c -> c.withNetwork(network).withNetworkAliases("kafka"))
                .build();
        kafka.start();

        postgres = new PostgreSQLContainer<>("postgres:16")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("outbox_test")
                .withUsername("outbox")
                .withPassword("outbox")
                .withCommand("postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_wal_senders=4",
                        "-c", "max_replication_slots=4");
        postgres.start();

        connect = DebeziumContainer.latestStable()
                .withNetwork(network)
                .withKafka(kafka);
        connect.start();

        registerConnector();

        return Map.of(
                "quarkus.datasource.db-kind", "postgresql",
                "quarkus.datasource.jdbc.url", postgres.getJdbcUrl(),
                "quarkus.datasource.username", postgres.getUsername(),
                "quarkus.datasource.password", postgres.getPassword(),
                "kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    private void registerConnector() {
        ConnectorConfiguration config = ConnectorConfiguration.create()
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("database.hostname", "postgres")
                .with("database.port", "5432")
                .with("database.user", postgres.getUsername())
                .with("database.password", postgres.getPassword())
                .with("database.dbname", postgres.getDatabaseName())
                .with("topic.prefix", "outbox")
                .with("table.include.list", "public.outbox_record")
                .with("slot.name", "outbox_slot")
                .with("plugin.name", "pgoutput")
                // remove-after-insert deletes every row: no tombstone for each delete.
                .with("tombstones.on.delete", "false");
        connect.registerConnector("outbox-connector", config);
        connect.ensureConnectorState("outbox-connector",
                io.debezium.testing.testcontainers.Connector.State.RUNNING);
    }

    @Override
    public void stop() {
        if (connect != null)
            connect.stop();
        if (postgres != null)
            postgres.stop();
        if (kafka != null)
            kafka.stop();
        if (network != null)
            network.close();
    }
}
