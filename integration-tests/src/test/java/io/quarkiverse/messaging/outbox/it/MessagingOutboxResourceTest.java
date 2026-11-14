package io.quarkiverse.messaging.outbox.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class MessagingOutboxResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/messaging-outbox")
                .then()
                .statusCode(200)
                .body(is("Hello messaging-outbox"));
    }
}
