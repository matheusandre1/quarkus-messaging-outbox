package io.quarkiverse.messaging.outbox.it;

import java.util.HashMap;
import java.util.Map;

import org.testcontainers.activemq.ArtemisContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.ToxiproxyContainer;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

public class AmqpToxiproxyResource implements QuarkusTestResourceLifecycleManager {

    private static final int AMQP_PORT = 61616;
    private static final int PG_PORT = 5432;

    private Network network;
    private ArtemisContainer artemis;
    private PostgreSQLContainer<?> postgres;
    private ToxiproxyContainer toxiproxy;

    static ToxiproxyContainer.ContainerProxy amqpProxy;
    static ToxiproxyContainer.ContainerProxy pgProxy;

    @Override
    public Map<String, String> start() {
        network = Network.newNetwork();

        artemis = new ArtemisContainer("apache/activemq-artemis:latest")
                .withNetwork(network)
                .withNetworkAliases("artemis");
        artemis.start();

        postgres = new PostgreSQLContainer<>("postgres:16")
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("outbox")
                .withUsername("test")
                .withPassword("test");
        postgres.start();

        toxiproxy = new ToxiproxyContainer()
                .withNetwork(network);
        toxiproxy.start();

        amqpProxy = toxiproxy.getProxy("artemis", AMQP_PORT);
        pgProxy = toxiproxy.getProxy("postgres", PG_PORT);

        Map<String, String> props = new HashMap<>();
        props.put("amqp-host", amqpProxy.getContainerIpAddress());
        props.put("amqp-port", String.valueOf(amqpProxy.getProxyPort()));
        props.put("amqp-username", artemis.getUser());
        props.put("amqp-password", artemis.getPassword());
        props.put("quarkus.devservices.enabled", "false");
        props.put("quarkus.datasource.db-kind", "postgresql");
        props.put("quarkus.datasource.jdbc.url",
                "jdbc:postgresql://" + pgProxy.getContainerIpAddress() + ":" + pgProxy.getProxyPort() + "/outbox");
        props.put("quarkus.datasource.username", "test");
        props.put("quarkus.datasource.password", "test");
        props.put("quarkus.hibernate-orm.schema-management.strategy", "drop-and-create");
        return props;
    }

    @Override
    public void stop() {
        if (toxiproxy != null) {
            toxiproxy.stop();
        }
        if (artemis != null) {
            artemis.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (network != null) {
            network.close();
        }
    }
}
