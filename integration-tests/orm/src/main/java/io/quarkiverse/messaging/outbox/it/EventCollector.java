package io.quarkiverse.messaging.outbox.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;

@ApplicationScoped
public class EventCollector {

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> receivedSubjects = new CopyOnWriteArrayList<>();

    @Incoming("events-in")
    public void consume(String payload, IncomingAmqpMetadata amqpMeta) {
        received.add(payload);
        if (amqpMeta != null && amqpMeta.getSubject() != null) {
            receivedSubjects.add(amqpMeta.getSubject());
        }
    }

    public List<String> getReceived() {
        return List.copyOf(received);
    }

    public List<String> getReceivedSubjects() {
        return List.copyOf(receivedSubjects);
    }

    public void clear() {
        received.clear();
        receivedSubjects.clear();
    }
}
