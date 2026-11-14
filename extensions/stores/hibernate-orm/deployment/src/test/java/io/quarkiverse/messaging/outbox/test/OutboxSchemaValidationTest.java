package io.quarkiverse.messaging.outbox.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.quarkiverse.messaging.outbox.spi.ClaimRequest;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxRecordEntity;
import io.quarkus.test.QuarkusExtensionTest;

/**
 * The DDL published in the documentation creates the schema Hibernate ORM expects.
 * <p>
 * Flyway runs the documented script for the database under test, and Hibernate ORM validates the schema at
 * startup instead of creating it. If the script and the entities disagree, the application does not start.
 * The test then writes, claims, retries and parks records, and takes the channel lease.
 * <p>
 * The script is read from {@code docs/modules/ROOT/examples/ddl/<db-kind>.sql}, the file the documentation
 * includes, so the two cannot drift apart.
 */
public class OutboxSchemaValidationTest {

    /** The database the tests run on. The MySQL test module sets it as a system property. */
    static final String DB_KIND = System.getProperty("quarkus.datasource.db-kind", "postgresql");

    @RegisterExtension
    static final QuarkusExtensionTest config = new QuarkusExtensionTest()
            .withApplicationRoot(jar -> jar
                    .addClasses(HibernateOrmStoreTest.TestService.class, CollectorConnector.class,
                            SimpleStringMapper.class)
                    .addAsResource(documentedDdl(), "db/outbox/V1__outbox.sql"))
            .overrideConfigKey("mp.messaging.outgoing.test-channel.connector", "test-collector")
            .overrideConfigKey("mp.messaging.outgoing.test-channel.outbox.relay.enabled", "false")
            .overrideConfigKey("quarkus.flyway.migrate-at-start", "true")
            .overrideConfigKey("quarkus.flyway.clean-at-start", "true")
            .overrideConfigKey("quarkus.flyway.locations", "db/outbox")
            .overrideConfigKey("quarkus.hibernate-orm.schema-management.strategy", "validate")
            .overrideConfigKey("quarkus.datasource.db-kind", "postgresql");

    @Inject
    HibernateOrmStoreTest.TestService service;

    @Test
    void documentedSchemaPassesValidationAndStoresRecords() {
        OutboxStore store = service.createStore();
        service.persistWithKey(store, "key-1", "first");
        service.persistWithKey(store, "key-1", "second");

        List<OutboxRecord> claimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(claimed).extracting(r -> new String(r.getPayload())).containsExactly("first", "second");
        assertThat(claimed.get(0).getSeq()).isLessThan(claimed.get(1).getSeq());

        RuntimeException longError = new RuntimeException("x".repeat(5000));
        store.retry(claimed.get(0), Instant.now(), longError).await().indefinitely();
        store.park(claimed.get(1), longError).await().indefinitely();
        // A retried record stays claimed by the relay that retries it: release it to read it back.
        store.release(List.of(claimed.get(0))).await().indefinitely();

        List<OutboxRecord> reclaimed = store.claim(
                new ClaimRequest(10, Duration.ofSeconds(30), true, Set.of()))
                .await().indefinitely();
        assertThat(reclaimed).hasSize(1);
        assertThat(reclaimed.get(0).getMsgKey()).isEqualTo("key-1");
        assertThat(reclaimed.get(0).getLastError()).hasSize(OutboxRecordEntity.LAST_ERROR_LENGTH);
        assertThat(store.holdLease(Duration.ofSeconds(30)).await().indefinitely()).isTrue();
        assertThat(store.stats().await().indefinitely().failed()).isEqualTo(1);
    }

    /** Finds the documented DDL by walking up from the working directory to the repository root. */
    private static File documentedDdl() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("docs/modules/ROOT/examples/ddl/" + DB_KIND + ".sql");
            if (Files.exists(candidate)) {
                return candidate.toFile();
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("No documented DDL for database kind '" + DB_KIND + "'");
    }
}
