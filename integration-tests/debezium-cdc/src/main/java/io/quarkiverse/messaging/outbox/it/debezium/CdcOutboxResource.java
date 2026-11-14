package io.quarkiverse.messaging.outbox.it.debezium;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkiverse.messaging.outbox.OutboxEmitter;

@Path("/cdc-outbox")
@ApplicationScoped
public class CdcOutboxResource {

    @Inject
    @Channel("cdc-channel")
    OutboxEmitter<String> emitter;

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
    }

    @Inject
    EntityManager em;

    /** Rows in the outbox table. With remove-after-insert there are none. */
    @GET
    @Path("/rows")
    @Transactional
    public long rows() {
        return em.createQuery("select count(e) from OutboxRecordEntity e", Long.class).getSingleResult();
    }

    @POST
    @Path("/send-with-key")
    @Transactional
    public void sendWithKey(@QueryParam("msg") String msg, @QueryParam("key") String key) {
        emitter.sendAndAwait(key, msg);
    }
}
