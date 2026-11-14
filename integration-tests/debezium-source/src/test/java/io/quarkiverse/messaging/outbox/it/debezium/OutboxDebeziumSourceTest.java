package io.quarkiverse.messaging.outbox.it.debezium;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;
import io.restassured.common.mapper.TypeRef;

/**
 * The outbox relayed from PostgreSQL's transaction log by the Debezium record source, to Kafka.
 */
@QuarkusTest
public class OutboxDebeziumSourceTest {

    /** The engine takes a snapshot and opens the slot at startup: allow for it on the first delivery. */
    static final Duration DELIVERY = Duration.ofSeconds(60);

    /**
     * Records pending when the engine starts come from its snapshot, in seq order, not commit order. Before the
     * first test, wait until the engine streams: a record sent now is delivered from the log.
     */
    static volatile boolean streaming;

    @BeforeEach
    void clear() {
        if (!streaming) {
            send("warm-up");
            await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).contains("warm-up"));
            streaming = true;
        }
        given().post("/cdc/clear").then().statusCode(204);
    }

    static List<CdcOutboxResource.Received> received() {
        return given().get("/cdc/received").then().statusCode(200).extract().as(new TypeRef<>() {
        });
    }

    static List<String> payloads() {
        return received().stream().map(CdcOutboxResource.Received::payload).toList();
    }

    static void send(String msg) {
        given().queryParam("msg", msg).post("/cdc/send").then().statusCode(204);
    }

    @Test
    void recordsAreDeliveredInOrderAndRemovedFromTheTable() {
        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            send("in-order-" + i);
            sent.add("in-order-" + i);
        }

        await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).containsAll(sent));
        assertThat(payloads().stream().filter(p -> p.startsWith("in-order-")).toList()).containsExactlyElementsOf(sent);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(
                Long.parseLong(given().get("/cdc/pending").then().extract().asString())).isZero());
    }

    /**
     * The first transaction to insert commits last. The polling relay would send it first, by insert order;
     * the transaction log has it second.
     */
    @Test
    void overlappingTransactionsAreDeliveredInCommitOrder() throws Exception {
        CompletableFuture<Void> slow = CompletableFuture.runAsync(() -> given()
                .queryParam("msg", "inserted-first").queryParam("holdMs", 2000)
                .post("/cdc/send-slow").then().statusCode(204));
        await().atMost(Duration.ofSeconds(10)).until(() -> Boolean.parseBoolean(
                given().get("/cdc/slow-inserted").then().statusCode(200).extract().asString()));

        send("committed-first");
        slow.get(15, TimeUnit.SECONDS);

        await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).contains("committed-first", "inserted-first"));
        assertThat(payloads().stream().filter(p -> p.endsWith("-first")).toList())
                .containsExactly("committed-first", "inserted-first");
    }

    @Test
    void aRolledBackTransactionSendsNothing() {
        given().queryParam("msg", "rolled-back").post("/cdc/send-rollback").then().statusCode(500);
        send("after-rollback");

        // The later record arrives, so the relay has read past the rolled-back transaction.
        await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).contains("after-rollback"));
        assertThat(payloads()).doesNotContain("rolled-back");
    }

    @Test
    void theKeyAndTheOutboxIdReachKafka() {
        given().queryParam("msg", "keyed").queryParam("key", "customer-42").post("/cdc/send").then().statusCode(204);

        await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).contains("keyed"));
        CdcOutboxResource.Received keyed = received().stream().filter(r -> r.payload().equals("keyed")).findFirst()
                .orElseThrow();
        assertThat(keyed.key()).isEqualTo("customer-42");
        assertThat(keyed.outboxId()).hasSize(36);
    }

    @Test
    void theHealthCheckReportsTheChannel() {
        send("health");
        await().atMost(DELIVERY).untilAsserted(() -> assertThat(payloads()).contains("health"));

        given().get("/q/health/ready").then().statusCode(200)
                .body(org.hamcrest.Matchers.containsString("cdc-events.pending"));
    }
}
