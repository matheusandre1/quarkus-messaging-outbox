package io.quarkiverse.messaging.outbox.it;

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

/** Sends object payloads through the outbox to Kafka, and collects them back as JSON text. */
@Path("/pojo")
@ApplicationScoped
public class PojoOutboxResource {

    @Inject
    @Channel("pojo-outbox")
    OutboxEmitter<OrderPlaced> emitter;

    private final List<String> received = new CopyOnWriteArrayList<>();

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("orderId") String orderId) {
        emitter.sendAndAwait(new OrderPlaced(orderId));
    }

    @POST
    @Path("/send-priority")
    @Transactional
    public void sendPriority(@QueryParam("orderId") String orderId) {
        emitter.sendAndAwait(new PriorityOrderPlaced(orderId));
    }

    @GET
    @Path("/received")
    public List<String> received() {
        return List.copyOf(received);
    }

    @POST
    @Path("/received/clear")
    public void clear() {
        received.clear();
    }

    @Incoming("pojo-in")
    public void consume(String json) {
        received.add(json);
    }
}
