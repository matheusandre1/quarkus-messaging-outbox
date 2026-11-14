package io.quarkiverse.messaging.outbox.it.debezium;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.apache.kafka.common.header.Header;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

/** Writes records through the outbox, relayed by the Debezium source to Kafka, and reads them back. */
@Path("/cdc")
@ApplicationScoped
public class CdcOutboxResource {

    /** What the consumer received, as key, payload and outbox id. */
    public record Received(String key, String payload, String outboxId) {
    }

    @Inject
    @Channel("cdc-events")
    OutboxEmitter<String> emitter;

    @Inject
    EntityManager em;

    private final List<Received> received = new CopyOnWriteArrayList<>();
    private final AtomicBoolean slowInserted = new AtomicBoolean();

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg, @QueryParam("key") String key) {
        if (key == null) {
            emitter.sendAndAwait(msg);
        } else {
            emitter.sendAndAwait(key, msg);
        }
    }

    @POST
    @Path("/send-rollback")
    @Transactional
    public void sendRollback(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
        throw new IllegalStateException("forced rollback");
    }

    /**
     * Inserts the record, then holds the transaction open for {@code holdMs} before committing. Another
     * transaction can insert after it and commit before it.
     */
    @POST
    @Path("/send-slow")
    @Transactional
    public void sendSlow(@QueryParam("msg") String msg, @QueryParam("holdMs") long holdMs) throws InterruptedException {
        emitter.sendAndAwait(msg);
        em.flush();
        slowInserted.set(true);
        Thread.sleep(holdMs);
    }

    @GET
    @Path("/slow-inserted")
    public boolean slowInserted() {
        return slowInserted.get();
    }

    /** Records still pending in the outbox table. Delivered ones are deleted. */
    @GET
    @Path("/pending")
    @Transactional
    public long pending() {
        return em.createQuery("select count(e) from OutboxRecordEntity e where e.status = 0", Long.class)
                .getSingleResult();
    }

    @GET
    @Path("/received")
    public List<Received> received() {
        return List.copyOf(received);
    }

    @POST
    @Path("/clear")
    public void clear() {
        received.clear();
        slowInserted.set(false);
    }

    @Incoming("cdc-in")
    public void consume(String payload, IncomingKafkaRecordMetadata<?, ?> kafka) {
        Header id = kafka.getHeaders().lastHeader("outbox-id");
        received.add(new Received(kafka.getKey() == null ? null : String.valueOf(kafka.getKey()), payload,
                id == null ? null : new String(id.value(), StandardCharsets.UTF_8)));
    }
}
