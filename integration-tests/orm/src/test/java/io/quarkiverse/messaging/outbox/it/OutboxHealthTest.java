package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
@QuarkusTestResource(AmqpToxiproxyResource.class)
public class OutboxHealthTest {

    @Test
    void e11_healthEndpointReportsUp() {
        given().when().get("/q/health/ready")
                .then()
                .statusCode(200)
                .body("status", is("UP"));
    }

    @Test
    void e11_healthContainsOutboxCheck() {
        given().when().get("/q/health/ready")
                .then()
                .statusCode(200)
                .body("checks.find { it.name == 'Outbox store' }.status", is("UP"));
    }
}
