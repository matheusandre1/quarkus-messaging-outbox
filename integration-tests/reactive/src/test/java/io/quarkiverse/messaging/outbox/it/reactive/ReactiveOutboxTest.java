package io.quarkiverse.messaging.outbox.it.reactive;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ReactiveOutboxTest {

    @BeforeEach
    void clear() {
        given().when().post("/reactive-outbox/received/clear").then().statusCode(204);
    }

    @Test
    void reactiveStoreDeliversMessage() {
        given().queryParam("msg", "reactive-hello")
                .when().post("/reactive-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            String countStr = given().when().get("/reactive-outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void sendWithoutTransaction_failsAndStoresNothing() {
        given().queryParam("msg", "no-tx")
                .when().post("/reactive-outbox/send-without-transaction")
                .then().statusCode(500);

        // Other tests share the consumer, so look for this message rather than an empty list.
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(
                () -> assertThat(given().when().get("/reactive-outbox/received")
                        .then().statusCode(200).extract().jsonPath().getList(".", String.class))
                        .doesNotContain("no-tx"));
    }

    @Test
    void committedTransaction_entityAndMessageBothPresent() {
        given().queryParam("msg", "committed")
                .when().post("/reactive-outbox/send-with-entity")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            int received = Integer.parseInt(given().when().get("/reactive-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(received).isGreaterThanOrEqualTo(1);
        });

        long customers = Long.parseLong(given().when().get("/reactive-outbox/customer/count")
                .then().statusCode(200).extract().body().asString());
        assertThat(customers).isGreaterThanOrEqualTo(1);
    }

    @Test
    void rolledBackTransaction_neitherEntityNorMessage() {
        given().queryParam("msg", "rolled-back")
                .when().post("/reactive-outbox/send-with-entity-rollback")
                .then().statusCode(500);

        // Wait a bit to make sure no message leaks through
        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            int received = Integer.parseInt(given().when().get("/reactive-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(received).isZero();
        });

        long customers = Long.parseLong(given().when().get("/reactive-outbox/customer/count")
                .then().statusCode(200).extract().body().asString());
        assertThat(customers).isZero();
    }
}
