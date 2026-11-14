package io.quarkiverse.messaging.outbox.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.inject.Singleton;

@Singleton
public class OutboxChannelTypes {

    private final Map<String, String> types = new ConcurrentHashMap<>();

    public void register(String channel, String payloadTypeName) {
        types.put(channel, payloadTypeName);
    }

    public String getPayloadTypeName(String channel) {
        return types.get(channel);
    }

    public Class<?> getPayloadType(String channel) {
        String name = types.get(channel);
        if (name == null) {
            return null;
        }
        try {
            return Class.forName(name, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Payload type not found: " + name, e);
        }
    }
}
