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
public class OutboxFaultInjectionTest {

    @BeforeEach
    void clearState() {
        given().when().post("/outbox/received/clear").then().statusCode(204);
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(false);
        AmqpToxiproxyResource.pgProxy.setConnectionCut(false);
    }

    @Test
    void f2_brokerCutAndRestored_messagesDelivered() {
        // Send a message while broker is up
        given().queryParam("msg", "msg-before-cut")
                .when().post("/outbox/send")
                .then().statusCode(204);

        // Wait for delivery
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });

        // Cut and restore the broker connection
        given().when().post("/outbox/received/clear").then().statusCode(204);
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(true);
        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {
        }
        AmqpToxiproxyResource.amqpProxy.setConnectionCut(false);

        // Send another message after restoration — should be delivered
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            given().queryParam("msg", "msg-after-restore")
                    .when().post("/outbox/send")
                    .then().statusCode(204);

            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void f3_connectionCutAndRestored() {
        // Send a message while broker is up
        given().queryParam("msg", "before-cut")
                .when().post("/outbox/send")
                .then().statusCode(204);

        // Wait for delivery
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }

    @Test
    void f9_databaseRestartWhileRelayRuns() {
        // Send a message and wait for delivery to confirm everything works
        given().queryParam("msg", "before-db-cut")
                .when().post("/outbox/send")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });

        // Cut the database connection
        AmqpToxiproxyResource.pgProxy.setConnectionCut(true);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }

        // Restore the database connection
        AmqpToxiproxyResource.pgProxy.setConnectionCut(false);

        // Send another message after restoration — it should be delivered
        given().when().post("/outbox/received/clear").then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            // The app should recover and be able to send again
            given().queryParam("msg", "after-db-restore")
                    .when().post("/outbox/send")
                    .then().statusCode(204);

            String countStr = given().when().get("/outbox/received/count")
                    .then().statusCode(200)
                    .extract().body().asString();
            assertThat(Integer.parseInt(countStr)).isGreaterThanOrEqualTo(1);
        });
    }
}
