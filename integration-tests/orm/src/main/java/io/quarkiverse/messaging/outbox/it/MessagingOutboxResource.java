package io.quarkiverse.messaging.outbox.it;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;
import org.hibernate.SessionFactory;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.amqp.OutgoingAmqpMetadata;

@Path("/outbox")
@ApplicationScoped
public class MessagingOutboxResource {

    @Inject
    @Channel("outbox-events")
    OutboxEmitter<String> emitter;

    @Inject
    EventCollector collector;

    @Inject
    SessionFactory sessionFactory;

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
    }

    @POST
    @Path("/send-batch")
    @Transactional
    public void sendBatch(@QueryParam("count") int count) {
        for (int i = 0; i < count; i++) {
            emitter.sendAndAwait("msg-" + i);
        }
    }

    @GET
    @Path("/received")
    public List<String> received() {
        return collector.getReceived();
    }

    @GET
    @Path("/received/count")
    public int receivedCount() {
        return collector.getReceived().size();
    }

    @POST
    @Path("/received/clear")
    public void clearReceived() {
        collector.clear();
    }

    @POST
    @Path("/send-traced")
    @Transactional
    public String sendTraced(@QueryParam("msg") String msg) {
        io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.current();
        String traceId = span.getSpanContext().getTraceId();
        emitter.sendAndAwait(msg);
        return traceId;
    }

    @POST
    @Path("/send-with-subject")
    @Transactional
    public void sendWithSubject(@QueryParam("msg") String msg, @QueryParam("subject") String subject) {
        var amqpMeta = OutgoingAmqpMetadata.builder()
                .withSubject(subject)
                .build();
        emitter.sendMessageAndAwait(Message.of(msg, Metadata.of(amqpMeta)));
    }

    @GET
    @Path("/received/subjects")
    public String receivedSubjects() {
        return String.join(",", collector.getReceivedSubjects());
    }

    @GET
    @Path("/outbox-table/headers")
    public String lastHeaders() {
        try (var session = sessionFactory.openStatelessSession()) {
            return session.createNativeQuery(
                    "select headers from outbox_record order by seq desc limit 1", String.class)
                    .uniqueResultOptional().orElse("no-records");
        }
    }

    @GET
    @Path("/outbox-table/claimed")
    public long claimedCount() {
        try (var session = sessionFactory.openStatelessSession()) {
            return session.createNativeQuery(
                    "select count(*) from outbox_record where claimed_by is not null", Long.class)
                    .uniqueResult();
        }
    }

    @GET
    @Path("/outbox-table/pending")
    public long pendingCount() {
        try (var session = sessionFactory.openStatelessSession()) {
            return session.createNativeQuery(
                    "select count(*) from outbox_record where status = 0", Long.class)
                    .uniqueResult();
        }
    }

    @GET
    @Path("/outbox-table/lease")
    public long leaseCount() {
        try (var session = sessionFactory.openStatelessSession()) {
            return session.createNativeQuery(
                    "select count(*) from outbox_lease", Long.class)
                    .uniqueResult();
        }
    }

    @GET
    public String hello() {
        return "Hello messaging-outbox";
    }
}
