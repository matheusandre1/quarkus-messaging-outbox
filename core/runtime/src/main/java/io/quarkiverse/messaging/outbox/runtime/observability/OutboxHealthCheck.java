package io.quarkiverse.messaging.outbox.runtime.observability;

import java.time.Duration;
import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import io.quarkiverse.messaging.outbox.runtime.OutboxEmitterFactory;
import io.quarkiverse.messaging.outbox.spi.OutboxStats;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;

/**
 * Reports DOWN when an outbox store cannot be queried, or when a channel's record source failed and waits to be
 * started again. Lag is reported as data and does not affect the status: a broker outage must not take the
 * application out of rotation, since accepting writes is the point of the outbox.
 */
@Readiness
@ApplicationScoped
public class OutboxHealthCheck implements HealthCheck {

    // The factory carries the @EmitterFactoryFor qualifier, so it is looked up with @Any.
    // Stores are created per channel by the factory and are not beans themselves.
    @Inject
    @Any
    Instance<OutboxEmitterFactory> factories;

    @Override
    public HealthCheckResponse call() {
        HealthCheckResponseBuilder builder = HealthCheckResponse.named("Outbox store");
        boolean sourceFailed = false;
        try {
            for (OutboxEmitterFactory factory : factories) {
                for (Map.Entry<String, Throwable> failure : factory.getSourceFailures().entrySet()) {
                    builder.withData(failure.getKey() + ".source-error", String.valueOf(failure.getValue()));
                    sourceFailed = true;
                }
                for (Map.Entry<String, OutboxStore> entry : factory.getStores().entrySet()) {
                    String channel = entry.getKey();
                    OutboxStats stats = entry.getValue().stats().await().atMost(Duration.ofSeconds(5));
                    builder.withData(channel + ".pending", stats.pending())
                            .withData(channel + ".failed", stats.failed())
                            .withData(channel + ".oldest-age-seconds", stats.oldestAge().toSeconds());
                }
            }
            if (sourceFailed) {
                builder.down();
            } else {
                builder.up();
            }
        } catch (Exception e) {
            builder.down().withData("error", String.valueOf(e.getMessage()));
        }
        return builder.build();
    }
}
