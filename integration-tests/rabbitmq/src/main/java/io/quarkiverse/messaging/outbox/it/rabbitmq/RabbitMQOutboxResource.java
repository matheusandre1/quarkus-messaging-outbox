package io.quarkiverse.messaging.outbox.it.rabbitmq;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.quarkiverse.messaging.outbox.OutboxEmitter;

@Path("/rabbitmq-outbox")
@ApplicationScoped
public class RabbitMQOutboxResource {

    @Inject
    @Channel("rabbitmq-outbox")
    OutboxEmitter<String> emitter;

    private final List<String> received = new CopyOnWriteArrayList<>();

    @Incoming("rabbitmq-in")
    public void consume(String payload) {
        received.add(payload);
    }

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
    }

    @GET
    @Path("/received/count")
    public int receivedCount() {
        return received.size();
    }

    @POST
    @Path("/received/clear")
    public void clearReceived() {
        received.clear();
    }
}
