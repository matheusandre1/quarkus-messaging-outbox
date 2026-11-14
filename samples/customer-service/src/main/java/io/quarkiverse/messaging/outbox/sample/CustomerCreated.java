package io.quarkiverse.messaging.outbox.sample;

public record CustomerCreated(Long customerId, String name) {
}
