package io.quarkiverse.messaging.outbox.it;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.amqp.IncomingAmqpMetadata;

@Path("/fallback-outbox")
@ApplicationScoped
public class FallbackOutboxResource {

    @Inject
    @Channel("fallback-events")
    OutboxEmitter<String> emitter;

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> receivedMessageIds = new CopyOnWriteArrayList<>();

    @Incoming("fallback-in")
    public void consume(String payload, IncomingAmqpMetadata amqpMeta) {
        received.add(payload);
        if (amqpMeta != null && amqpMeta.getId() != null) {
            receivedMessageIds.add(amqpMeta.getId());
        }
    }

    @POST
    @Path("/send")
    public void send(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
    }

    @GET
    @Path("/received/count")
    public int receivedCount() {
        return received.size();
    }

    @GET
    @Path("/received/message-ids")
    public String receivedMessageIds() {
        return String.join(",", receivedMessageIds);
    }

    @POST
    @Path("/received/clear")
    public void clearReceived() {
        received.clear();
        receivedMessageIds.clear();
    }
}
