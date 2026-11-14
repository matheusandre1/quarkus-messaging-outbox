package io.quarkiverse.messaging.outbox.it;

import io.quarkus.test.junit.QuarkusIntegrationTest;

/** Runs the object payload test against the packaged application, a native executable with -Dnative. */
@QuarkusIntegrationTest
public class OutboxPojoIT extends OutboxPojoTest {
}
