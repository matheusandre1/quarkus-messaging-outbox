# Quarkus Messaging Outbox

[![Version](https://img.shields.io/maven-central/v/io.quarkiverse.messaging.outbox/quarkus-messaging-outbox?logo=apache-maven&style=flat-square)](https://central.sonatype.com/artifact/io.quarkiverse.messaging.outbox/quarkus-messaging-outbox-parent)

A Quarkus extension that implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox) for Quarkus Messaging. `OutboxEmitter<T>` stores outgoing messages in the caller's database transaction and relays them to any messaging connector (Kafka, AMQP, RabbitMQ) with at-least-once delivery.

## Features

- **Drop-in replacement for `MutinyEmitter`** with connector metadata round-trip for Kafka, AMQP and RabbitMQ
- **Atomic messaging**: no message for a rolled-back transaction, no lost message for a committed one
- **Relay ordering**: `strict` sends one record at a time in sequence order, `key` orders per key with keys in parallel, `none` sends all in parallel
- **Fallback mode**: send directly, store to outbox only on failure
- **Hibernate ORM and Hibernate Reactive** stores, any database Hibernate supports
- **Multi-instance safe**: lease-based coordination for ordered channels, skip-locked claims for parallel channels
- **Outbox ID on every message** for consumer-side deduplication (`outbox-id` Kafka header, AMQP/RabbitMQ message id)
- **Debezium source** (experimental): relay from the transaction log instead of polling, with commit-order delivery
- **Works with external CDC**: set `relay.enabled=false` and use Debezium Kafka Connect as the relay
- **Native image** support

## Quick start

Import the BOM and add the extension. The store, mapper, and connector modules activate automatically based on your existing Quarkus dependencies:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.quarkiverse.messaging.outbox</groupId>
            <artifactId>quarkus-messaging-outbox-bom</artifactId>
            <version>${quarkus-messaging-outbox.version}</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>io.quarkiverse.messaging.outbox</groupId>
        <artifactId>quarkus-messaging-outbox</artifactId>
    </dependency>
</dependencies>
```

Inject and use:

```java
@ApplicationScoped
public class CustomerService {

    @Inject
    @Channel("customer-events")
    OutboxEmitter<CustomerCreated> events;

    @Transactional
    public void create(Customer customer) {
        customer.persist();
        events.sendAndAwait(new CustomerCreated(customer.id));
    }
}
```

The channel keeps its normal connector configuration:

```properties
mp.messaging.outgoing.customer-events.connector=smallrye-kafka
mp.messaging.outgoing.customer-events.topic=customer-events
```

## How it works

1. `sendAndAwait` persists the message as a row in the outbox table, inside the caller's transaction.
2. After the transaction commits, the relay claims the row and sends it to the connector.
3. On broker ack, the row is deleted. On nack, it is retried with exponential backoff.
4. If the transaction rolls back, the row is never visible and nothing is sent.

## Extensions

| Extension | Activates when | Purpose |
| --- | --- | --- |
| Hibernate ORM store | `quarkus-hibernate-orm` + JTA | Writes and claims outbox rows via JDBC |
| Hibernate Reactive store | `quarkus-hibernate-reactive` | Writes and claims outbox rows via reactive SQL |
| Jackson mapper | `quarkus-jackson` | Serializes payloads as JSON |
| Kafka connector | `quarkus-messaging-kafka` | Preserves Kafka headers, keys, partition, topic |
| AMQP connector | `quarkus-messaging-amqp` | Preserves AMQP properties |
| RabbitMQ connector | `quarkus-messaging-rabbitmq` | Preserves RabbitMQ headers and properties |
| Debezium source | `quarkus-messaging-outbox-debezium` | Relays from the transaction log instead of polling |

## Configuration

Channel-level keys sit under `mp.messaging.outgoing.<channel>.outbox.`:

| Key | Default | Description |
| --- | --- | --- |
| `mode` | `transactional` | `transactional` or `fallback` |
| `ordering` | `strict` | `strict`, `key`, or `none` |
| `relay.enabled` | `true` | `false` for external CDC relay |
| `relay.source` | `polling` | `polling` or `debezium` |
| `retention.mode` | `delete` | `delete` or `keep` |
| `max-attempts` | `-1` | Attempts before parking (`-1` = forever) |
| `poll-interval` | `5s` | Recovery poll period |
| `batch-size` | `100` | Records per claim |
| `lease-duration` | `30s` | Claim and channel lease TTL |

Build-time keys sit under `quarkus.messaging-outbox.`:

| Key | Default | Description |
| --- | --- | --- |
| `table-name` | `outbox_record` | Outbox record table name |
| `lease-table-name` | `outbox_lease` | Lease table name |
| `persistence-unit` | `<default>` | Persistence unit for outbox entities |

## Documentation

Full documentation is available at <https://docs.quarkiverse.io/quarkus-messaging-outbox/dev/index.html>.

## License

This project is licensed under the [Apache License 2.0](LICENSE).
