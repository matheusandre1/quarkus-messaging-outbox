package io.quarkiverse.messaging.outbox.it.rabbitmq;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class RabbitMQOutboxTest {

    @BeforeEach
    void clearState() {
        given().when().post("/rabbitmq-outbox/received/clear").then().statusCode(204);
    }

    @Test
    void happyPath_messageDeliveredViaRabbitMQ() {
        given().queryParam("msg", "rabbitmq-hello")
                .when().post("/rabbitmq-outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            long count = Long.parseLong(given().when().get("/rabbitmq-outbox/received/count")
                    .then().statusCode(200).extract().body().asString());
            assertThat(count).isGreaterThanOrEqualTo(1);
        });
    }
}
