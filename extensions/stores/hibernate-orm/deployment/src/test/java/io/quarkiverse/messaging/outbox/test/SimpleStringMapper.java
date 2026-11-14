package io.quarkiverse.messaging.outbox.test;

import java.lang.reflect.Type;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.OutboxMetadata;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;

@ApplicationScoped
public class SimpleStringMapper implements OutboxRecordMapper {

    @Override
    public OutboxRecord toRecord(String channel, Message<?> message) {
        OutboxRecord record = new OutboxRecord();
        record.setId(UUID.randomUUID());
        record.setChannel(channel);
        record.setPayload(String.valueOf(message.getPayload()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
        record.setPayloadType("java.lang.String");
        record.setStatus(OutboxRecord.STATUS_PENDING);
        message.getMetadata(OutboxMetadata.class)
                .ifPresent(meta -> meta.getKey().ifPresent(record::setMsgKey));
        return record;
    }

    @Override
    public Message<?> toMessage(OutboxRecord record, Type payloadType) {
        return Message.of(record.getPayload(), Metadata.of(OutboxMetadata.withKey(record.getMsgKey())));
    }
}
