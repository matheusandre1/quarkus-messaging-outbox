package io.quarkiverse.messaging.outbox.sample;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.notNullValue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class CustomerServiceTest {

    @Test
    void createCustomerAndReceiveEvent() {
        // Create a customer — this persists the entity AND sends an outbox event
        given().queryParam("name", "Ada").queryParam("email", "ada@example.com")
                .when().post("/customers")
                .then().statusCode(200)
                .body("id", notNullValue());

        // The event should flow through: outbox table → relay → Kafka → consumer
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            String countStr = given().when().get("/customers/events/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }
}
