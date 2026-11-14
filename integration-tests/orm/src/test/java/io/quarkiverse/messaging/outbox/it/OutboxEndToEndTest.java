package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxEndToEndTest {

    @BeforeEach
    void clearState() {
        given().when().post("/outbox/received/clear").then().statusCode(204);
    }

    @Test
    void f1_happyPathMessageDelivered() {
        given().queryParam("msg", "hello-outbox")
                .when().post("/outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }
}
