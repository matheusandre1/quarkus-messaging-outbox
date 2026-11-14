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
public class OutboxFallbackTest {

    @BeforeEach
    void clearState() {
        given().when().post("/fallback-outbox/received/clear").then().statusCode(204);
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(false);
    }

    @Test
    void f10_fallbackHappyPath_directDelivery() {
        given().queryParam("msg", "direct-msg")
                .when().post("/fallback-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/fallback-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void f10_fallbackBrokerDown_storedAndDeliveredAfterRecovery() {
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(true);

        given().queryParam("msg", "fallback-msg")
                .when().post("/fallback-outbox/send")
                .then().statusCode(204);

        AmqpToxiproxyResource.amqpProxy.setConnectionCut(false);

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/fallback-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void fallback_outboxIdReachesConsumer() {
        given().queryParam("msg", "id-msg")
                .when().post("/fallback-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/fallback-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            String ids = given().when().get("/fallback-outbox/received/message-ids")
                    .then().statusCode(200).extract().body().asString();
            assertThat(ids).isNotEmpty();
            UUID.fromString(ids.split(",")[0]);
        });
    }
}
