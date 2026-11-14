package io.quarkiverse.messaging.outbox.it.reactive;

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
import org.hibernate.reactive.mutiny.Mutiny;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.mutiny.Uni;

@Path("/reactive-outbox")
@ApplicationScoped
public class ReactiveOutboxResource {

    @Inject
    @Channel("reactive-events")
    OutboxEmitter<String> emitter;

    @Inject
    Mutiny.SessionFactory sf;

    /** Bound to the reactive @Transactional transaction; sf.withSession must not be used inside one. */
    @Inject
    Mutiny.Session session;

    private final List<String> received = new CopyOnWriteArrayList<>();

    @Incoming("reactive-events")
    public void consume(Object payload) {
        received.add(String.valueOf(payload));
    }

    @POST
    @Path("/send")
    public Uni<Void> send(@QueryParam("msg") String msg) {
        return sf.withTransaction(session -> emitter.send(msg));
    }

    /** Fails: in transactional mode the outbox requires the caller's transaction. */
    @POST
    @Path("/send-without-transaction")
    public Uni<Void> sendWithoutTransaction(@QueryParam("msg") String msg) {
        return emitter.send(msg);
    }

    @POST
    @Path("/send-with-entity")
    public Uni<Void> sendWithEntity(@QueryParam("msg") String msg) {
        return sf.withTransaction(session -> {
            ReactiveCustomer customer = new ReactiveCustomer(msg);
            return session.persist(customer)
                    .chain(() -> emitter.send(msg));
        });
    }

    @POST
    @Path("/send-with-entity-rollback")
    public Uni<Void> sendWithEntityRollback(@QueryParam("msg") String msg) {
        return sf.withTransaction(session -> {
            ReactiveCustomer customer = new ReactiveCustomer(msg);
            return session.persist(customer)
                    .chain(() -> emitter.send(msg))
                    .chain(() -> Uni.createFrom().failure(new RuntimeException("forced rollback")));
        });
    }

    /**
     * The entity and the outbox record are written in a transaction started by reactive {@code @Transactional},
     * not by the application calling {@code sf.withTransaction}.
     */
    @POST
    @Path("/transactional/send-with-entity")
    @Transactional
    public Uni<Void> transactionalSendWithEntity(@QueryParam("msg") String msg) {
        return session.persist(new ReactiveCustomer(msg))
                .chain(() -> emitter.send(msg));
    }

    @POST
    @Path("/transactional/send-with-entity-rollback")
    @Transactional
    public Uni<Void> transactionalSendWithEntityRollback(@QueryParam("msg") String msg) {
        return session.persist(new ReactiveCustomer(msg))
                .chain(() -> emitter.send(msg))
                .chain(() -> Uni.createFrom().failure(new RuntimeException("forced rollback")));
    }

    @GET
    @Path("/received")
    public List<String> receivedMessages() {
        return List.copyOf(received);
    }

    @GET
    @Path("/customer/count-by-name")
    public Uni<Long> customerCountByName(@QueryParam("name") String name) {
        return sf.withSession(session -> session
                .createQuery("select count(c) from ReactiveCustomer c where c.name = :name", Long.class)
                .setParameter("name", name)
                .getSingleResult());
    }

    @GET
    @Path("/received/count")
    public int receivedCount() {
        return received.size();
    }

    @GET
    @Path("/customer/count")
    public Uni<Long> customerCount() {
        return sf.withSession(session -> session.createQuery("select count(c) from ReactiveCustomer c", Long.class)
                .getSingleResult());
    }

    @POST
    @Path("/received/clear")
    public void clearReceived() {
        received.clear();
    }
}
