# Milestone 0 brief: wiring spike

Build an empty Quarkiverse extension whose `OutboxEmitter` forwards messages straight to the channel, and report what you learn about emitter wiring on Quarkus 4. No store, no relay and no database are part of this milestone.

The design is in the Design tab of this document. Read its Summary, Target platform, User-facing API and Extension structure sections before you start. Where this brief and the design disagree, this brief wins for milestone 0.

## Environment

- Java 21 and Maven 3.9 or later.
- Quarkus 4.0, at the newest 4.0 version that resolves from Maven Central. On 14 November 2026 that was 4.0.0.CR1.
- No Docker is needed. Tests use the in-memory connector.

## Step 1: bootstrap

Run this in an empty directory:

```bash
mvn io.quarkus.platform:quarkus-maven-plugin:<quarkus-4-version>:create-extension -N \
    -DgroupId=io.quarkiverse.messaging-outbox \
    -DextensionId=messaging-outbox \
    -U
```

- **Pin the plugin version.** Without a version Maven picks the latest release of the plugin, which may still be a Quarkus 3 version and would generate a Quarkus 3 extension.
- **If the `io.quarkus.platform` plugin has no 4.0 version yet**, use `io.quarkus:quarkus-maven-plugin` at the same version and say so in the report.
- **Check the result.** The generated POMs must reference Quarkus 4.0, and the compiler release must be 21. The name field of quarkus-extension.yaml in the runtime module must be Quarkus Messaging Outbox. Set it if the generator produced something else.
- **Record what was generated:** group id, artifact ids, Java package and module list. If the package is not `io.quarkiverse.messaging.outbox`, keep what the generator produced and report it.

## Step 2: dependencies

| Module | Dependency | Scope |
| --- | --- | --- |
| `runtime` | `io.quarkus:quarkus-messaging` | compile |
| `deployment` | `io.quarkus:quarkus-messaging-deployment` | compile |
| `deployment` | `io.quarkus:quarkus-junit-internal` | test |
| `deployment` | `io.smallrye.reactive:smallrye-reactive-messaging-in-memory` | test |
| `deployment` | AssertJ and Awaitility | test |

Versions come from the Quarkus BOM. Do not pin a SmallRye Reactive Messaging version yourself.

## Step 3: code

### Runtime

| Class | What it does in this milestone |
| --- | --- |
| `OutboxEmitter<T>` | The interface from the design, with all six methods |
| `OutboxMetadata` | Holds an optional `String` key. `send(key, payload)` attaches it to the message |
| `OutboxEmitterImpl<T>` | Implements `OutboxEmitter<T>` and `MessagePublisherProvider<Object>`. Forwards each message to the channel, and completes `send` when the message is acked downstream |
| `OutboxEmitterFactory` | `@EmitterFactoryFor(OutboxEmitter.class)`, both `createEmitter` methods, and the `@Produces @Channel("")` producer shown in the design |
| `OutboxChannelTypes` | Maps each channel name to the payload type of its injection point, filled at build time |

In the three-argument `createEmitter`, keep the received `Config` (or the fact that it was `null`) so a test can inspect it.

### Deployment

- Register the factory as an unremovable bean.
- Find every `OutboxEmitter<T>` injection point with `@Channel`, and record channel name and `T` into `OutboxChannelTypes`.
- Fail the build with a clear message for a raw `OutboxEmitter` injection point.
- Produce the feature build item named `messaging-outbox`.

## Step 4: tests

All tests are `QuarkusExtensionTest` classes in the `deployment` module, with the channel bound to the in-memory connector.

| Test | Setup | Passes when |
| --- | --- | --- |
| Wiring (E1) | A bean injects `OutboxEmitter<String>` on `customer-events` and sends one payload | The application starts and the in-memory sink receives the payload |
| Key | `send("k1", payload)` | The received message carries `OutboxMetadata` with key `k1` |
| Raw type (E2) | A bean injects raw `OutboxEmitter` | The build fails, and the message names the injection point |
| Payload type | `OutboxEmitter<CustomerCreated>` on `customer-events` | `OutboxChannelTypes` returns `CustomerCreated` for that channel |
| Config visible | `mp.messaging.outgoing.customer-events.outbox.store=probe` | The factory's `Config` returns `probe` for `outbox.store`. Also record whether startup logs an unknown-property warning |
| No connector | The channel is consumed by an `@Incoming` method in the application, with no connector | The application starts. Record whether the `Config` was `null` |
| Coexistence (E9) | An `OutboxEmitter` and a standard `Emitter` on two channels | Both deliver |

## Questions the report must answer

1. Does a factory from this extension get wired with no change in Quarkus or SmallRye Reactive Messaging? If not, what exactly failed?
2. Are `outbox.*` keys readable from the `Config` passed to `createEmitter`? Under which exact key names?
3. What does the factory receive when the channel has no outgoing connector?
4. Do unknown `outbox.*` keys cause warnings or errors at startup or at build time?
5. Which mechanism did you use to pass payload types from build time to runtime, and why?
6. Which coordinates, package and display name did the generator produce?
7. Where did the design document not match Quarkus 4.0 or SmallRye Reactive Messaging 5.0? Give the class, the expected signature and the actual one.

## Out of scope

- `OutboxStore`, `OutboxRelay`, entities, Hibernate dependencies, lease, ordering, retries, fallback mode.
- Connector bridges, metrics, health, Dev UI.
- Any change to Quarkus or SmallRye Reactive Messaging sources.
- Creating or pushing to a remote repository, and publishing artifacts.

## Rules

- **Do not work around a mismatch silently.** If an API named in the design does not exist or behaves differently, stop that item, describe it in the report, and continue with the rest.
- **Do not invent configuration keys or public API** beyond what this brief and the design name.
- **Keep the change small.** No refactoring of generated files beyond what the steps need.

## Done when

- `mvn -B verify` passes from the repository root.
- All seven tests pass.
- `SPIKE-REPORT.md` at the repository root answers the seven questions, with file paths and the relevant log lines.
