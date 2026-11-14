package io.quarkiverse.messaging.outbox;

import java.util.Optional;

public class OutboxMetadata {

    private final String key;

    private OutboxMetadata(String key) {
        this.key = key;
    }

    public static OutboxMetadata withKey(String key) {
        return new OutboxMetadata(key);
    }

    public Optional<String> getKey() {
        return Optional.ofNullable(key);
    }
}
