package io.quarkiverse.messaging.outbox.sample;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CustomerEventLogger {

    private static final Logger LOG = Logger.getLogger(CustomerEventLogger.class);

    private final List<String> received = new CopyOnWriteArrayList<>();

    @Incoming("customer-events-in")
    public void onCustomerCreated(String event) {
        LOG.infof("Customer event received: %s", event);
        received.add(event);
    }

    public List<String> getReceived() {
        return List.copyOf(received);
    }
}
