package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxMetadataRoundTripTest {

    @BeforeEach
    void clearState() {
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(false);
        AmqpToxiproxyResource.pgProxy.setConnectionCut(false);
        given().when().post("/outbox/received/clear").then().statusCode(204);
        given().when().post("/kafka-outbox/received/clear").then().statusCode(204);
    }

    @Test
    void kafkaKey_survivesOutboxRoundTrip() {
        given().queryParam("msg", "keyed-msg")
                .queryParam("key", "customer-42")
                .when().post("/kafka-outbox/send-with-key")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/kafka-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String keys = given().when().get("/kafka-outbox/received/keys")
                    .then().statusCode(200).extract().body().asString();
            assertThat(keys).contains("customer-42");
        });
    }

    @Test
    void kafkaRecord_keyAndValueExtracted() {
        given().queryParam("key", "order-99")
                .queryParam("msg", "record-payload")
                .when().post("/kafka-outbox/send-record")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/kafka-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String keys = given().when().get("/kafka-outbox/received/keys")
                    .then().statusCode(200).extract().body().asString();
            assertThat(keys).contains("order-99");
        });
    }

    @Test
    void kafkaHeaders_surviveOutboxRoundTrip() {
        given().queryParam("msg", "with-headers")
                .queryParam("correlationId", "corr-123")
                .when().post("/kafka-outbox/send-with-headers")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/kafka-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String headers = given().when().get("/kafka-outbox/received/headers")
                    .then().statusCode(200).extract().body().asString();
            assertThat(headers).contains("corr-123");
        });
    }

    @Test
    void kafkaOutboxId_alwaysPresent() {
        given().queryParam("msg", "id-check")
                .when().post("/kafka-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/kafka-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String outboxIds = given().when().get("/kafka-outbox/received/outbox-ids")
                    .then().statusCode(200).extract().body().asString();
            assertThat(outboxIds).isNotEmpty();
            UUID.fromString(outboxIds.split(",")[0]);
        });
    }

    @Test
    void amqpSubject_survivesOutboxRoundTrip() {
        given().queryParam("msg", "with-subject")
                .queryParam("subject", "customer.created")
                .when().post("/outbox/send-with-subject")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String subjects = given().when().get("/outbox/received/subjects")
                    .then().statusCode(200).extract().body().asString();
            assertThat(subjects).contains("customer.created");
        });
    }
}
