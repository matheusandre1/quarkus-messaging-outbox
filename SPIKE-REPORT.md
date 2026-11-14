# Milestone 0 Spike Report

## Generator output

| Item | Value |
| --- | --- |
| Plugin | `io.quarkus.platform:quarkus-maven-plugin:4.0.0.CR1` |
| Group id | `io.quarkiverse.messaging-outbox` |
| Artifact ids | `quarkus-messaging-outbox-parent`, `quarkus-messaging-outbox` (runtime), `quarkus-messaging-outbox-deployment` |
| Java package | `io.quarkiverse.messaging.outbox` |
| Modules | `runtime`, `deployment`, `docs`, `integration-tests` |
| Quarkus version | 4.0.0.CR1 |
| SmallRye Reactive Messaging version | 5.0.0-RC2 (from BOM) |
| Compiler release | 21 |
| Display name | `Quarkus Messaging Outbox` (set manually; generator produced `Messaging Outbox`) |

## Answers to the seven questions

### 1. Does a factory from this extension get wired with no change in Quarkus or SmallRye Reactive Messaging?

**Yes.** The `@EmitterFactoryFor(OutboxEmitter.class)` annotation on `OutboxEmitterFactory` is discovered by `WiringProcessor.extractComponents` in the Quarkus messaging deployment module. The factory's `createEmitter` methods are called, and the emitter's `getPublisher()` is wired as the channel's publisher. No changes to Quarkus or SmallRye Reactive Messaging were needed.

The three-argument `createEmitter(EmitterConfiguration, long, Config)` is a `default` method on `EmitterFactory` added in SmallRye Reactive Messaging 5.0.0-RC2 and is called when the channel has an outgoing connector configured.

Relevant files:
- `runtime/src/main/java/io/quarkiverse/messaging/outbox/runtime/OutboxEmitterFactory.java`
- `runtime/src/main/java/io/quarkiverse/messaging/outbox/runtime/OutboxEmitterImpl.java`

### 2. Are `outbox.*` keys readable from the `Config` passed to `createEmitter`? Under which exact key names?

**Yes.** The `Config` passed to the three-argument `createEmitter` exposes channel-scoped keys with the `mp.messaging.outgoing.<channel>.` prefix stripped. Setting `mp.messaging.outgoing.customer-events.outbox.store=probe` in application config makes it readable as:

```java
channelConfig.getOptionalValue("outbox.store", String.class) // returns Optional.of("probe")
```

The key name in the `Config` is `outbox.store`, not the full `mp.messaging.outgoing.customer-events.outbox.store` path.

Test: `OutboxConfigVisibleTest.outboxConfigKeysAreReadableFromChannelConfig`

### 3. What does the factory receive when the channel has no outgoing connector?

**The two-argument `createEmitter(EmitterConfiguration, long)` is called instead.** The three-argument overload is never called, and the `Config` parameter is never passed. The factory stores `null` for that channel in its config map.

This matches the Javadoc of the three-argument `createEmitter`, which says `channelConfig` is "null when the channel is not connected to an outgoing connector." In practice, SmallRye simply calls the two-argument form (which is the only abstract method), and the three-argument default delegates to it.

Test: `OutboxNoConnectorTest.configIsNullWithNoConnector`

### 4. Do unknown `outbox.*` keys cause warnings or errors at startup or at build time?

**No.** Setting `mp.messaging.outgoing.customer-events.outbox.store=probe` (a key the extension does not yet consume) does not produce any warning or error at either build time or runtime. The `Config` object passed to `createEmitter` is a standard MicroProfile Config view, and SmallRye does not validate which keys an emitter factory reads.

Test: `OutboxConfigVisibleTest.noUnknownPropertyWarningForOutboxKeys`

### 5. Which mechanism did you use to pass payload types from build time to runtime, and why?

**A `@Recorder` that populates a singleton bean through `BeanContainer`.**

At build time, `MessagingOutboxProcessor.validateAndCapturePayloadTypes` scans every `OutboxEmitter<T>` injection point, extracts the payload type `T` and the `@Channel` name from the Jandex index, and produces an `OutboxChannelPayloadBuildItem` for each.

A `@Record(STATIC_INIT)` build step then calls `OutboxChannelTypesRecorder.register(BeanContainer, String, String)` for each item. The recorder looks up the `OutboxChannelTypes` singleton bean from the container and calls `register(channel, payloadTypeName)`.

This is the standard Quarkus pattern for transferring build-time data to runtime. The alternative — a synthetic bean with a recorded supplier — would also work, but populating an existing bean is simpler when only a map needs to be filled.

Relevant files:
- `deployment/src/main/java/io/quarkiverse/messaging/outbox/deployment/MessagingOutboxProcessor.java` (build steps)
- `deployment/src/main/java/io/quarkiverse/messaging/outbox/deployment/OutboxChannelPayloadBuildItem.java`
- `runtime/src/main/java/io/quarkiverse/messaging/outbox/runtime/OutboxChannelTypesRecorder.java`
- `runtime/src/main/java/io/quarkiverse/messaging/outbox/runtime/OutboxChannelTypes.java`

Note: `@Record` and `ExecutionTime` are deprecated in Quarkus 4.0.0.CR1. The replacement API should be confirmed before milestone 1.

### 6. Which coordinates, package and display name did the generator produce?

| Item | Generated value |
| --- | --- |
| Group id | `io.quarkiverse.messaging-outbox` |
| Parent artifact id | `quarkus-messaging-outbox-parent` |
| Runtime artifact id | `quarkus-messaging-outbox` |
| Deployment artifact id | `quarkus-messaging-outbox-deployment` |
| Java package | `io.quarkiverse.messaging.outbox` |
| Display name (quarkus-extension.yaml) | `Messaging Outbox` (changed to `Quarkus Messaging Outbox`) |
| Detected layout | `quarkiverse` |
| Modules | `runtime`, `deployment`, `docs`, `integration-tests` |

The `docs` and `integration-tests` modules are activated by profiles and excluded from release builds.

### 7. Where did the design document not match Quarkus 4.0 or SmallRye Reactive Messaging 5.0?

| Item | Design says | Actual in 4.0.0.CR1 / 5.0.0-RC2 | Effect |
| --- | --- | --- | --- |
| `ChannelProducer` package | `io.smallrye.reactive.messaging.extension.ChannelProducer` | `io.smallrye.reactive.messaging.providers.extension.ChannelProducer` | Import path changed; the class and its `getChannelName` method are unchanged |
| `@Record` / `ExecutionTime` | Used for build-time recorders | Deprecated in Quarkus 4.0.0.CR1 (compiles with warnings) | Works but should be migrated to the replacement API |
| `smallrye-reactive-messaging-in-memory` | Used as the test connector artifact | Relocated to `smallrye-reactive-messaging-testing` in SRM 5.0. Maven resolves the relocation, but the JAR is not discoverable from `QuarkusExtensionTest` without explicit setup | Extension tests use an inline `CollectorConnector` instead. The in-memory connector is still usable in `@QuarkusTest` application tests |
| `Multi.subscribe().asPublisher()` | Implied by Mutiny 2.x patterns | Does not exist in Mutiny 3.x. `Multi<T>` directly extends `Flow.Publisher<T>` | `getPublisher()` returns the Multi directly |
| `Message.withAckWithMetadata` / `withNackWithMetadata` | Not used in design, but likely in SRM examples | Not available in MP Reactive Messaging 4.0 | Used standard `withAck(Supplier)` / `withNack(Function)` |

All mismatches were resolved without changes to Quarkus or SmallRye Reactive Messaging.

## Test results

All tests pass with `mvn -B verify` from the repository root.

| Test | Class | Result |
| --- | --- | --- |
| Wiring (E1) | `OutboxWiringTest` | Pass |
| Key | `OutboxKeyTest` | Pass |
| Raw type (E2) | `OutboxRawTypeTest` | Pass |
| Payload type | `OutboxPayloadTypeTest` | Pass |
| Config visible | `OutboxConfigVisibleTest` (2 tests) | Pass |
| No connector | `OutboxNoConnectorTest` (2 tests) | Pass |
| Coexistence (E9) | `OutboxCoexistenceTest` | Pass |

```
Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
```

## Observations

- The `@EmitterFactoryFor` qualifier on the factory bean means it cannot be injected with `@Default`. Tests inject it with `@Any`. The `@Produces @Channel("") produce(InjectionPoint)` method handles the user-facing injection through `ChannelRegistry`.
- A channel with an `OutboxEmitter` and no connector works: SmallRye wires the emitter's publisher to any `@Incoming` method on the same channel name.
- `OutboxEmitter` and a standard `Emitter` coexist on different channels without interference.
- The extension feature name `messaging-outbox` appears in the startup log: `Installed features: [cdi, messaging, messaging-outbox, ...]`.
