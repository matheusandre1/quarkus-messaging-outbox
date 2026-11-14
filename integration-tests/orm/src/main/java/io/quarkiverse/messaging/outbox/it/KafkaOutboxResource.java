package io.quarkiverse.messaging.outbox.it;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@Path("/kafka-outbox")
@ApplicationScoped
public class KafkaOutboxResource {

    @Inject
    @Channel("kafka-outbox")
    OutboxEmitter<String> emitter;

    @Inject
    @Channel("kafka-record-outbox")
    OutboxEmitter<Record<String, String>> recordEmitter;

    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> receivedKeys = new CopyOnWriteArrayList<>();
    private final List<String> receivedHeaders = new CopyOnWriteArrayList<>();
    private final List<String> receivedOutboxIds = new CopyOnWriteArrayList<>();

    @Incoming("kafka-in")
    public void consume(String payload, IncomingKafkaRecordMetadata<?, ?> kafkaMeta) {
        received.add(payload);
        if (kafkaMeta != null) {
            if (kafkaMeta.getKey() != null) {
                receivedKeys.add(String.valueOf(kafkaMeta.getKey()));
            }
            Header h = kafkaMeta.getHeaders().lastHeader("correlation-id");
            if (h != null) {
                receivedHeaders.add(new String(h.value(), StandardCharsets.UTF_8));
            }
            Header outboxId = kafkaMeta.getHeaders().lastHeader("outbox-id");
            if (outboxId != null) {
                receivedOutboxIds.add(new String(outboxId.value(), StandardCharsets.UTF_8));
            }
        }
    }

    @POST
    @Path("/send")
    @Transactional
    public void send(@QueryParam("msg") String msg) {
        emitter.sendAndAwait(msg);
    }

    @POST
    @Path("/send-with-key")
    @Transactional
    public void sendWithKey(@QueryParam("msg") String msg, @QueryParam("key") String key) {
        emitter.sendAndAwait(key, msg);
    }

    @POST
    @Path("/send-record")
    @Transactional
    public void sendRecord(@QueryParam("key") String key, @QueryParam("msg") String msg) {
        recordEmitter.sendAndAwait(Record.of(key, msg));
    }

    @POST
    @Path("/send-with-headers")
    @Transactional
    public void sendWithHeaders(@QueryParam("msg") String msg, @QueryParam("correlationId") String correlationId) {
        var kafkaMeta = OutgoingKafkaRecordMetadata.builder()
                .withHeaders(new RecordHeaders(new RecordHeader[] {
                        new RecordHeader("correlation-id", correlationId.getBytes(StandardCharsets.UTF_8))
                }))
                .build();
        emitter.sendMessageAndAwait(Message.of(msg, Metadata.of(kafkaMeta)));
    }

    @GET
    @Path("/received/count")
    public int receivedCount() {
        return received.size();
    }

    @GET
    @Path("/received/keys")
    public String receivedKeys() {
        return String.join(",", receivedKeys);
    }

    @GET
    @Path("/received/headers")
    public String receivedHeaders() {
        return String.join(",", receivedHeaders);
    }

    @GET
    @Path("/received/outbox-ids")
    public String receivedOutboxIds() {
        return String.join(",", receivedOutboxIds);
    }

    @POST
    @Path("/received/clear")
    public void clearReceived() {
        received.clear();
        receivedKeys.clear();
        receivedHeaders.clear();
        receivedOutboxIds.clear();
    }
}
