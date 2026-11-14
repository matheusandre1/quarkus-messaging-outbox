package io.quarkiverse.messaging.outbox.rabbitmq;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.rabbitmq.OutgoingRabbitMQMetadata;
import io.smallrye.reactive.messaging.rabbitmq.RabbitMQConnector;

@Identifier(RabbitMQConnector.CONNECTOR_NAME)
@ApplicationScoped
public class RabbitMQOutboxMetadataConverter implements OutboxMetadataConverter {

    private static final String PREFIX = "rabbitmq.";

    @Override
    public Optional<String> extractKey(Message<?> message) {
        return message.getMetadata(OutgoingRabbitMQMetadata.class)
                .map(OutgoingRabbitMQMetadata::getRoutingKey)
                .filter(k -> k != null && !k.isEmpty());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> extract(Message<?> message) {
        var opt = message.getMetadata(OutgoingRabbitMQMetadata.class);
        if (opt.isEmpty()) {
            return Map.of();
        }
        OutgoingRabbitMQMetadata meta = opt.get();
        Map<String, Object> map = new LinkedHashMap<>();
        var props = meta.getProperties();

        if (meta.getRoutingKey() != null) {
            map.put(PREFIX + "routing-key", meta.getRoutingKey());
        }
        meta.getExchange().ifPresent(e -> map.put(PREFIX + "exchange", e));
        if (props.getContentType() != null) {
            map.put(PREFIX + "content-type", props.getContentType());
        }
        if (props.getContentEncoding() != null) {
            map.put(PREFIX + "content-encoding", props.getContentEncoding());
        }
        if (props.getCorrelationId() != null) {
            map.put(PREFIX + "correlation-id", props.getCorrelationId());
        }
        if (props.getReplyTo() != null) {
            map.put(PREFIX + "reply-to", props.getReplyTo());
        }
        if (props.getExpiration() != null) {
            map.put(PREFIX + "expiration", props.getExpiration());
        }
        if (props.getMessageId() != null) {
            map.put(PREFIX + "message-id", props.getMessageId());
        }
        if (props.getType() != null) {
            map.put(PREFIX + "type", props.getType());
        }
        if (props.getAppId() != null) {
            map.put(PREFIX + "app-id", props.getAppId());
        }
        if (props.getPriority() != null) {
            map.put(PREFIX + "priority", props.getPriority());
        }
        if (props.getDeliveryMode() != null) {
            map.put(PREFIX + "delivery-mode", props.getDeliveryMode());
        }
        if (props.getHeaders() != null && !props.getHeaders().isEmpty()) {
            Map<String, Object> headers = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : props.getHeaders().entrySet()) {
                headers.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            map.put(PREFIX + "headers", headers);
        }
        return map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Metadata restore(OutboxRecord record) {
        var builder = OutgoingRabbitMQMetadata.builder();
        Map<String, Object> stored = record.getHeaders() != null ? record.getHeaders() : Map.of();
        String key = record.getMsgKey();

        Object routingKey = stored.get(PREFIX + "routing-key");
        if (routingKey instanceof String rk) {
            builder.withRoutingKey(rk);
        } else if (key != null) {
            builder.withRoutingKey(key);
        }

        Object exchange = stored.get(PREFIX + "exchange");
        if (exchange instanceof String e) {
            builder.withExchange(e);
        }

        Object contentType = stored.get(PREFIX + "content-type");
        if (contentType instanceof String ct) {
            builder.withContentType(ct);
        }

        Object contentEncoding = stored.get(PREFIX + "content-encoding");
        if (contentEncoding instanceof String ce) {
            builder.withContentEncoding(ce);
        }

        Object correlationId = stored.get(PREFIX + "correlation-id");
        if (correlationId instanceof String cid) {
            builder.withCorrelationId(cid);
        }

        Object replyTo = stored.get(PREFIX + "reply-to");
        if (replyTo instanceof String rt) {
            builder.withReplyTo(rt);
        }

        Object expiration = stored.get(PREFIX + "expiration");
        if (expiration instanceof String exp) {
            builder.withExpiration(exp);
        }

        Object messageId = stored.get(PREFIX + "message-id");
        if (messageId instanceof String mid) {
            builder.withMessageId(mid);
        } else if (record.getId() != null) {
            builder.withMessageId(record.getId().toString());
        }

        Object type = stored.get(PREFIX + "type");
        if (type instanceof String t) {
            builder.withType(t);
        }

        Object appId = stored.get(PREFIX + "app-id");
        if (appId instanceof String aid) {
            builder.withAppId(aid);
        }

        Object priority = stored.get(PREFIX + "priority");
        if (priority instanceof Number p) {
            builder.withPriority(p.intValue());
        }

        Object deliveryMode = stored.get(PREFIX + "delivery-mode");
        if (deliveryMode instanceof Number dm) {
            builder.withDeliveryMode(dm.intValue());
        }

        Object headers = stored.get(PREFIX + "headers");
        if (headers instanceof Map<?, ?> headerMap) {
            for (Map.Entry<?, ?> entry : headerMap.entrySet()) {
                builder.withHeader(String.valueOf(entry.getKey()), entry.getValue());
            }
        }
        if (record.getId() != null) {
            builder.withHeader("outbox-id", record.getId().toString());
        }

        return Metadata.of(builder.build());
    }
}
