package io.quarkiverse.messaging.outbox.it.reactive;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

/**
 * The outbox write joins a transaction started by reactive {@code @Transactional}
 * (quarkus-reactive-transactions): it commits and rolls back with the entity.
 * <p>
 * The rollback case is the one that tells: if the store opened a transaction of its own, the message would
 * be delivered although the entity was rolled back.
 */
@QuarkusTest
public class ReactiveTransactionalTest {

    private static List<String> received() {
        return given().when().get("/reactive-outbox/received")
                .then().statusCode(200).extract().jsonPath().getList(".", String.class);
    }

    private static long customers(String name) {
        return Long.parseLong(given().queryParam("name", name)
                .when().get("/reactive-outbox/customer/count-by-name")
                .then().statusCode(200).extract().body().asString());
    }

    @Test
    void committed_entityAndMessageBothPresent() {
        given().queryParam("msg", "tx-committed")
                .when().post("/reactive-outbox/transactional/send-with-entity")
                .then().statusCode(204);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(received()).contains("tx-committed"));
        assertThat(customers("tx-committed")).isEqualTo(1);
    }

    @Test
    void rolledBack_neitherEntityNorMessage() {
        given().queryParam("msg", "tx-rolled-back")
                .when().post("/reactive-outbox/transactional/send-with-entity-rollback")
                .then().statusCode(500);

        await().during(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(received()).doesNotContain("tx-rolled-back"));
        assertThat(customers("tx-rolled-back")).isZero();
    }
}
