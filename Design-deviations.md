# Design deviations

Differences between [Design.md](Design.md) and the current implementation, as of 17 November 2026. Design.md is the original proposal and is not updated; this file records where the implementation differs.

## Extension structure

**Design:** one Quarkiverse project with `runtime`, `deployment`, `integration-tests` and `docs` modules.

**Implementation:** multi-module project split by concern:

| Module | Purpose |
| --- | --- |
| `core/runtime` | `OutboxEmitterImpl`, `OutboxRelay`, `OutboxConfig`, `TracingOutboxMetadataConverter`, `OutboxHealthCheck` |
| `core/runtime-spi` | `OutboxEmitter`, `OutboxMetadata`, `OutboxStore`, `OutboxRecord`, `OutboxRecordMapper`, `OutboxMetadataConverter`, `OutboxStoreContext`, `ClaimRequest`, `OutboxStats`, `LeaseLostException` |
| `core/deployment` | Core build steps, `OutboxBuildTimeConfig`, `OutboxTableNameBuildItem` |
| `extensions/stores/persistence/runtime` | `OutboxRecordEntity`, `OutboxLeaseEntity`, `JsonMapConverter`, `OutboxTableNameInitListener` |
| `extensions/stores/hibernate-orm/{runtime,deployment}` | Hibernate ORM store and its build steps |
| `extensions/stores/hibernate-reactive/{runtime,deployment}` | Hibernate Reactive store and its build steps |
| `extensions/mappers/jackson/{runtime,deployment}` | Jackson-based `OutboxRecordMapper` |
| `extensions/connectors/kafka/{runtime,deployment}` | Kafka `OutboxMetadataConverter` |
| `extensions/connectors/amqp/{runtime,deployment}` | AMQP `OutboxMetadataConverter` |
| `extensions/connectors/rabbitmq/{runtime,deployment}` | RabbitMQ `OutboxMetadataConverter` |
| `bom/` | BOM generated with `bom-builder3` |

**Reason:** the SPI, entities, mapper, and connector support are independent axes. Splitting them lets applications pull in only what they use, and lets store and connector extensions have proper conditional dependencies.

## Connector metadata: OutboxMetadataConverter replaces OutgoingInterceptor bridges

**Design:** per-connector `OutgoingInterceptor` bridges that translate `OutboxMetadata` into connector metadata in `beforeMessageSend`.

**Implementation:** `OutboxMetadataConverter` SPI. Each connector provides a converter annotated with `@Identifier(connectorName)`. The emitter calls converters on the write side (`extract`, `extractKey`, `extractValue`) and the relay calls them on the read side (`restore`).

```java
public interface OutboxMetadataConverter extends Prioritized {
    default Optional<String> extractKey(Message<?> message) { return Optional.empty(); }
    default Optional<Object> extractValue(Message<?> message) { return Optional.empty(); }
    Map<String, Object> extract(Message<?> message);
    Metadata restore(OutboxRecord record);
}
```

Cross-cutting converters (like tracing) have no `@Identifier` and apply to all channels. Connector-specific converters are selected by matching the channel's connector name.

**Reason:** `OutgoingInterceptor` bridges don't fire on relay-emitted messages because the SmallRye bridge infrastructure doesn't intercept them. Moving metadata handling to the converter lets both the write side and the relay round-trip connector metadata (Kafka headers, AMQP properties, etc.) through the outbox store.

## RabbitMQ connector support

**Design:** not mentioned. Only AMQP and Kafka bridges listed.

**Implementation:** `RabbitMQOutboxMetadataConverter` with `@Identifier(RabbitMQConnector.CONNECTOR_NAME)`. Stores and restores routing key, exchange, content-type, headers, priority, delivery mode, etc.

## OutboxStore SPI changes

**Design:**
```java
Uni<Void> persist(OutboxRecord record);
Uni<Void> delivered(OutboxRecord record);
```

**Implementation:**
```java
Uni<Void> persist(OutboxRecord record, Runnable afterCommit);
Uni<Void> delivered(OutboxRecord record, boolean keep);
```

- `persist` takes a `Runnable afterCommit` callback. The Hibernate ORM store registers it on the JTA transaction synchronization. The Hibernate Reactive store has no commit callback and runs it `signal-delay` after the write. The design had the commit signal in `OutboxStoreContext`, but the signal is per-persist, not per-store.
- `delivered` takes a `boolean keep` parameter. When `retention=keep`, the record is marked `SENT` instead of deleted.

## OutboxStoreContext

**Design:** carries channel name, channel config, relay instance id, and the commit signal.

**Implementation:** carries channel name, channel config, and relay instance id. No commit signal — the signal is passed as the `afterCommit` callback on each `persist` call.

## OutboxRecord headers

**Design:** `headers` column is `text (JSON)`, `OutboxRecord` has string headers.

**Implementation:** `OutboxRecord.headers` is `Map<String, Object>`. The entity uses a `JsonMapConverter` (`AttributeConverter<Map<String, Object>, String>`) backed by Vert.x `JsonObject` for JSON serialization.

**Reason:** connector metadata (Kafka headers with byte[] values, AMQP application properties with typed values) needs richer types than flat strings.

## Lease fencing

**Design:** an `epoch` column on `outbox_lease` incremented at each acquisition, checked in every relay write.

**Implementation:** fencing uses `owner` and `expiresAt` on `outbox_lease`. Each relay write checks that `lease.owner` matches the relay's instance id. No epoch column.

Two more rules keep the order when the lease changes hands:
- A claim locks the lease row (`PESSIMISTIC_READ`) and checks the owner in the same transaction. A relay that lost the lease cannot claim, and a takeover waits for a claim in progress.
- `holdLease(lease, acquire)`: when a relay that does not hold the lease acquires it, the same transaction clears the claims on the channel's pending records. Records claimed by the previous holder, or by the relay itself before a lease loss, would otherwise be skipped until their claims expire, and the records behind them sent first.

**Reason:** owner-based fencing is simpler and sufficient. A stale holder whose owner doesn't match the lease is rejected. The race window between lease expiry and the stale holder's next write is the same with or without an epoch.

## Mapper is not a CDI default bean

**Design:** mentions a default mapper, no specific CDI qualifier mechanism.

**Implementation:** `JacksonOutboxRecordMapper` uses `@Reserve @Priority(0)` (CDI 5.0), replacing the deprecated `@DefaultBean`. This lets applications override the mapper by providing their own bean without needing `@Alternative`.

## OutboxRecordMapper: no metadata methods

**Design:** mapper has `toRecord` and `toMessage`.

**Implementation:** same contract. Metadata extraction and restoration is handled by `OutboxMetadataConverter`, not the mapper. The mapper only serializes and deserializes the payload.

## Tracing

**Design:** "the mapper writes the current trace context into the headers at `send`."

**Implementation:** `TracingOutboxMetadataConverter` is a cross-cutting `OutboxMetadataConverter` (no `@Identifier`). It captures `traceparent` into the headers on the write side and restores it as `TracingMetadata` on the relay side. The mapper is not involved.

## No direct-forward mode

**Design:** not explicitly stated, but the factory `createEmitter` without channel config returns a "defaults" config.

**Implementation:** the emitter fails fast if no store or mapper is found. There is no bypass mode where messages skip the outbox.

## Kafka key handling

**Design:** "The key is a `String` in version 1."

**Implementation:** the key stored in `msg_key` is always a `String`. However, `KafkaOutboxMetadataConverter` preserves the original Kafka key type (Integer, Long, UUID, Short) through `kafka.key-type` and `kafka.key-value` entries in the headers map. The relay restores the typed key on the outgoing Kafka record. Non-string keys are serialized to string for storage and deserialized back.

## Record\<K,V\> support

**Design:** not mentioned.

**Implementation:** `OutboxMetadataConverter.extractValue(Message<?>)` unwraps `Record<K,V>` payloads. The key is extracted via `extractKey`, the value via `extractValue`, and only the value is serialized as the payload. The Kafka converter implements both methods for `Record`-typed messages.

## Build-time configuration

**Design:** `quarkus.messaging-outbox.table-name` and `quarkus.messaging-outbox.lease-table-name`.

**Implementation:** also adds `quarkus.messaging-outbox.persistence-unit` (default `<default>`). The persistence unit name flows through `OutboxTableNameBuildItem` to the store deployment processors, which use it for entity registration (`AdditionalJpaModelBuildItem`) and table name override (`HibernateOrmIntegrationStaticConfiguredBuildItem`).

## Configurable table names: metadata override instead of entity generation

**Design:** does not specify how table names are overridden.

**Implementation:** uses `HibernateOrmIntegrationStaticInitListener.onMetadataInitialized()` to modify `PersistentClass.getTable().setName()` at STATIC_INIT time. No XML mappings, no Gizmo class generation. Index names are also adjusted when the table name changes.

## Emitter factory

**Design:** shows a `@Produces @Typed(OutboxEmitter.class) @Channel("") <T> OutboxEmitter<T> produce(InjectionPoint)` method.

**Implementation:** `OutboxEmitterFactory` has a `produce` method with the same shape. The emitter is created by `createEmitter(EmitterConfiguration, long, Config)` and registered with the `ChannelRegistry`.

## Conditional extension dependencies

**Design:** not mentioned.

**Implementation:** the Hibernate ORM store extension uses `dependencyCondition` on `quarkus-narayana-jta` in its `quarkus-extension.yaml`. This means the extension is only activated when JTA is on the classpath. The `TransactionSynchronizationRegistry` injection is also optional (`Instance<TSR>`) to handle edge cases. `quarkus-agroal` and `quarkus-narayana-jta` are excluded from the transitive `quarkus-hibernate-orm` dependency to avoid pulling them into reactive-only applications.

## Relay split

**Design:** one relay class per channel.

**Implementation:** the relay is split into five classes in `io.quarkiverse.messaging.outbox.runtime.relay`:
- `OutboxRelay` — owns the Vert.x context and wires source, dispatcher, and lifecycle
- `PollingRecordSource` — claim query, poll timer, commit signal, claim renewal, channel lease, purge
- `Dispatcher` — routes records to key groups, sends them downstream, reports each ack or nack
- `KeyGroup` — FIFO queue for one ordering key, sends one record at a time, retries in place
- `OrderingPolicy` — enum with the differences between strict, key, and none ordering

## Config key rename

**Design:** `outbox.retention` for `delete` or `keep`.

**Implementation:** renamed to `outbox.retention.mode` so that `outbox.retention` can be a YAML section alongside `outbox.retention.period` and `outbox.retention.purge-interval`.

## Record table key

**Design:** `id` (UUID) is the primary key, and `seq` is a database-generated sequence.

**Implementation:** `seq` is the primary key, an identity column, because JPA cannot generate a value for a
column that is not the identifier. The UUID is the unique column `outbox_id`. Identity is portable across
databases, and it is assigned at insert, unlike Hibernate's sequence generators, which hand out
pre-allocated blocks per instance.

## Record source SPI

**Design:** the relay polls the outbox table.

**Implementation:** the relay reads records from an `OutboxRecordSource` (public SPI in `core/runtime-spi`). `PollingRecordSource` is the default. `outbox.relay.source` names another `OutboxRecordSource.Factory` bean by `@Identifier`, for example a future Debezium engine source. An unknown name fails at startup.

What the relay provides to a source:
- `OutboxRecordSourceContext` gives the channel, its configuration, the store, the clock, an executor and timers (`schedule`, `schedulePeriodically`) on the relay's context.
- The `Uni`s a source returns may complete on any thread: the relay moves back to its context.
- `Listener.failed(Throwable)`: the relay logs the failure, reports the channel down in the health check, closes the source and starts it again after a delay growing from `backoff.initial` to `backoff.max`.
- The javadoc of `OutboxRecordSource` states when records handed back through `released` may be handed out again: when due under `ordering=none`, and only once the key is no longer busy under `ordering=key`.

## Hibernate Reactive store and transactions

**Design:** `persist` uses the reactive session bound to the caller's Vert.x context.

**Implementation:**
- Without reactive `@Transactional`, `persist` takes the caller's session with `sf.withSession` and requires `currentTransaction()`, like the Hibernate ORM store requires a JTA transaction. `require-transaction=false` lets it write in a transaction of its own. The default is the same as in the ORM store: required, except in fallback mode.
- Under reactive `@Transactional` (quarkus-reactive-transactions, preview), the interceptor owns the transaction, and `sf.withSession` would close its connection while the transaction is live (HR000090). The store detects the interceptor's marker on the context (`TransactionalInterceptorBase.TRANSACTIONAL_METHOD_KEY`) and writes with the injected `Mutiny.Session`.
- quarkus-reactive-transactions has no public API for "is a transaction active" or for an after-commit callback. Both would let the store drop the internal key and the delayed signal.

## Per-channel persistence unit

**Design:** `persistence-unit` per channel.

**Implementation:** the Hibernate ORM store reads `outbox.persistence-unit` per channel. The build-time `quarkus.messaging-outbox.persistence-unit` decides which unit the entities belong to, so the two must agree. The Hibernate Reactive store uses the default unit only.

## Column sizes

**Design:** does not size the columns.

**Implementation:** `channel`, `msg_key`, `payload_type` and `claimed_by` use the default 255 characters. `last_error` is 1000 characters, `OutboxRecordEntity.LAST_ERROR_LENGTH`, and the stores truncate the error message to it. Payload and headers are unbounded (`bytea`/`text` on PostgreSQL, `longblob`/`longtext` on MySQL).

## Schema scripts

**Design:** DDL for PostgreSQL, MariaDB, MySQL, SQL Server and Oracle.

**Implementation:** DDL for PostgreSQL and MySQL in `docs/modules/ROOT/examples/ddl`, included in the documentation. `OutboxSchemaValidationTest` applies each script with Flyway and starts Hibernate ORM with schema validation.

## Remove after insert

**Design:** with the relay disabled, Debezium drains the table.

**Implementation:** `outbox.remove-after-insert=true` deletes the record in the transaction that writes it, as Debezium's Quarkus outbox extension does by default. The insert stays in the transaction log for an external CDC relay, and the table stays empty. It requires `outbox.relay.enabled=false`. Both stores persist then remove; the identity key makes `persist()` run the INSERT at once, so the pair is never cancelled before reaching the database. `integration-tests/debezium-cdc` checks the insert reaches Kafka and the table stays empty.

## Not yet implemented

The following items from the design are not yet implemented:

- **Dev UI card**
- **Metrics** (gauges and counters)
- **Health `max-age` turning sustained lag into DOWN**
- **Performance benchmark**
- **Ack timeout.** A record whose message is never acked or nacked stays in flight, and its claim is renewed.
- **Tests on MariaDB, SQL Server and Oracle**, and DDL for them.
- **Ordering by commit with the polling source.** `seq` is assigned at insert, so overlapping transactions can be delivered out of commit order. The documentation states the limit. The Debezium source does not have it for records committed while it streams; records already pending when its engine starts come from the snapshot in `seq` order.

## Implemented since the design

- **Native image:** payload types of `OutboxEmitter<T>` injection points, their type arguments, subclasses and implementations are registered for reflection. `OutboxPojoIT` runs natively.
- **MySQL:** `integration-tests/hibernate-orm-mysql` runs the database-sensitive store tests on MySQL.
- **Documentation:** Antora module in `docs`.
- **Debezium record source (experimental):** `extensions/sources/debezium`, an `OutboxRecordSource` on the Debezium embedded engine, selected with `outbox.relay.source=debezium`. PostgreSQL and MySQL, with defaults chosen from `quarkus.datasource.db-kind`; the application adds the connector of its database. JVM only. Commit order, no polling. It holds the channel lease to be the slot's only reader, lets the engine move past a batch only when all its records are settled, and keeps offsets in memory with a snapshot of pending records at start. Engine failures are reported with `Listener.failed`, and the relay restarts the source.
- **Warnings on nack and park:** a warning on the first nack of a record and every 10th, naming what waits behind it in `strict` and `key` modes, and one when it is parked.

## Open questions resolved

| Question | Resolution |
| --- | --- |
| Config prefix | Confirmed: `outbox.*` keys are readable from the channel config passed to `createEmitter` |
| One table or one per channel | One shared table. Per-channel tables rejected as too complex (would need distinct entity classes per channel) |
| Typed keys | Non-string Kafka keys are preserved through converter headers, not stored as typed values in `msg_key` |
