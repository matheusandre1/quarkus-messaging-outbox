package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxTracingTest {

    @Test
    void f12_traceContextCapturedDuringSend() {
        String traceId = given().queryParam("msg", "traced-msg")
                .when().post("/outbox/send-traced")
                .then().statusCode(200)
                .extract().body().asString();

        assertThat(traceId).isNotBlank();
        assertThat(traceId).hasSize(32);
        assertThat(traceId).doesNotMatch("0+");
    }
}
