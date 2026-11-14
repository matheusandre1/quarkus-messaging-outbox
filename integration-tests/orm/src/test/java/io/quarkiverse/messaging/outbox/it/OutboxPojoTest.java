package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

/** Object payloads, including a subclass of the emitter's type, survive the round trip through the outbox. */
@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxPojoTest {

    @BeforeEach
    void clearState() {
        given().when().post("/pojo/received/clear").then().statusCode(204);
    }

    @Test
    void objectPayloadsAreDelivered() {
        given().queryParam("orderId", "order-1").when().post("/pojo/send").then().statusCode(204);
        given().queryParam("orderId", "order-2").when().post("/pojo/send-priority").then().statusCode(204);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            List<String> received = given().when().get("/pojo/received")
                    .then().statusCode(200)
                    .extract().jsonPath().getList(".", String.class);
            assertThat(received).anyMatch(json -> json.contains("\"orderId\":\"order-1\""));
            assertThat(received).anyMatch(json -> json.contains("\"orderId\":\"order-2\"")
                    && json.contains("\"priority\":true"));
        });
    }
}
