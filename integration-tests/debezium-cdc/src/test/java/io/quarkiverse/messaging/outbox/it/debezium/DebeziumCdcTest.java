package io.quarkiverse.messaging.outbox.it.debezium;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/**
 * Proves that the outbox write side works with an external Debezium CDC relay.
 * <p>
 * The built-in relay is disabled ({@code outbox.relay.enabled=false}). PostgreSQL runs with
 * {@code wal_level=logical}, Kafka Connect runs the Debezium PostgreSQL connector, and the test
 * consumes from the Kafka topic Debezium publishes to.
 * <p>
 * Records arrive as raw Debezium change event JSON envelopes. The payload is base64-encoded in the
 * {@code after.payload} field. The outbox id and key are fields in the {@code after} object, not
 * Kafka headers — the connector metadata converter does not run when the relay is off.
 */
@QuarkusTest
@QuarkusTestResource(DebeziumCdcTestResource.class)
public class DebeziumCdcTest {

    @Inject
    CdcConsumer consumer;

    @BeforeEach
    void clear() {
        consumer.clear();
    }

    @Test
    void recordWrittenByOutboxEmitter_appearsOnKafkaViaCdc() {
        given().queryParam("msg", "cdc-hello")
                .when().post("/cdc-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(consumer.events()).anyMatch(e -> e.contains("\"channel\":\"cdc-channel\"")));
    }

    @Test
    void recordWithKey_appearsOnKafkaViaCdc() {
        given().queryParam("msg", "cdc-keyed").queryParam("key", "order-123")
                .when().post("/cdc-outbox/send-with-key")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(consumer.events()).anyMatch(e -> e.contains("\"msg_key\":\"order-123\"")));
    }

    @Test
    void outboxId_presentInCdcEvent() {
        given().queryParam("msg", "cdc-id-check")
                .when().post("/cdc-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(
                () -> assertThat(consumer.events()).anyMatch(e -> e.contains("\"outbox_id\":\"")));
    }

    /**
     * With outbox.remove-after-insert, the row is deleted in the transaction that writes it. Debezium still sees
     * the insert in the WAL, and the table stays empty.
     */
    @Test
    void removeAfterInsert_insertReachesKafkaAndTheTableStaysEmpty() {
        given().queryParam("msg", "cdc-removed")
                .when().post("/cdc-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(consumer.events())
                .anyMatch(e -> e.contains("\"op\":\"c\"") && e.contains("\"channel\":\"cdc-channel\"")));
        assertThat(Long.parseLong(given().get("/cdc-outbox/rows").then().statusCode(200).extract().asString()))
                .isZero();
    }

    @ApplicationScoped
    public static class CdcConsumer {

        // Written by the Kafka consumer thread, read by the test thread.
        private final List<String> events = new CopyOnWriteArrayList<>();

        @Incoming("cdc-in")
        public void consume(String event) {
            events.add(event);
        }

        public List<String> events() {
            return List.copyOf(events);
        }

        public void clear() {
            events.clear();
        }
    }
}
