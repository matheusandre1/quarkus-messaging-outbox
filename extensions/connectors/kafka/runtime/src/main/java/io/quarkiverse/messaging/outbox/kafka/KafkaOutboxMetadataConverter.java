package io.quarkiverse.messaging.outbox.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.smallrye.common.annotation.Identifier;
import io.smallrye.reactive.messaging.kafka.KafkaConnector;
import io.smallrye.reactive.messaging.kafka.Record;
import io.smallrye.reactive.messaging.kafka.api.OutgoingKafkaRecordMetadata;

@Identifier(KafkaConnector.CONNECTOR_NAME)
@ApplicationScoped
public class KafkaOutboxMetadataConverter implements OutboxMetadataConverter {

    private static final String PREFIX = "kafka.";
    private static final String BYTES_KEY_TYPE = "bytes";

    @Override
    public Optional<String> extractKey(Message<?> message) {
        // The outbox key is a string. A typed Kafka key is stored in string form here, so that key ordering
        // applies to it, and its type is kept in the headers so the original key is restored on the record.
        Object key = kafkaKey(message);
        return key == null ? Optional.empty() : Optional.of(keyAsString(key));
    }

    private static Object kafkaKey(Message<?> message) {
        if (message.getPayload() instanceof Record<?, ?> record && record.key() != null) {
            return record.key();
        }
        return message.getMetadata(OutgoingKafkaRecordMetadata.class)
                .map(meta -> (Object) meta.getKey())
                .orElse(null);
    }

    private static String keyAsString(Object key) {
        return key instanceof byte[] bytes ? Base64.getEncoder().encodeToString(bytes) : String.valueOf(key);
    }

    @Override
    public Optional<Object> extractValue(Message<?> message) {
        if (message.getPayload() instanceof Record<?, ?> record) {
            return Optional.of(record.value());
        }
        return Optional.empty();
    }

    @Override
    public Map<String, Object> extract(Message<?> message) {
        Map<String, Object> map = new LinkedHashMap<>();

        // Called with the message as sent, so a Record<K, V> payload still carries its key here.
        Object key = kafkaKey(message);
        if (key != null && !(key instanceof String)) {
            map.put(PREFIX + "key-value", keyAsString(key));
            map.put(PREFIX + "key-type", key instanceof byte[] ? BYTES_KEY_TYPE : key.getClass().getName());
        }

        var opt = message.getMetadata(OutgoingKafkaRecordMetadata.class);
        if (opt.isEmpty()) {
            return map;
        }
        OutgoingKafkaRecordMetadata<?> meta = opt.get();
        if (meta.getTopic() != null) {
            map.put(PREFIX + "topic", meta.getTopic());
        }
        if (meta.getPartition() >= 0) {
            map.put(PREFIX + "partition", meta.getPartition());
        }
        if (meta.getTimestamp() != null) {
            map.put(PREFIX + "timestamp", meta.getTimestamp().toEpochMilli());
        }
        if (meta.getHeaders() != null) {
            Map<String, String> headers = new LinkedHashMap<>();
            for (Header h : meta.getHeaders()) {
                if (h.value() != null) {
                    headers.put(h.key(), Base64.getEncoder().encodeToString(h.value()));
                }
            }
            if (!headers.isEmpty()) {
                map.put(PREFIX + "headers", headers);
            }
        }
        return map;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Metadata restore(OutboxRecord record) {
        var builder = OutgoingKafkaRecordMetadata.builder();
        Map<String, Object> stored = record.getHeaders() != null ? record.getHeaders() : Map.of();
        String key = record.getMsgKey();

        Object storedKeyValue = stored.get(PREFIX + "key-value");
        Object storedKeyType = stored.get(PREFIX + "key-type");
        if (storedKeyValue instanceof String kv && storedKeyType instanceof String kt) {
            builder.withKey(parseKey(kv, kt));
        } else if (key != null) {
            builder.withKey(key);
        }

        Object topic = stored.get(PREFIX + "topic");
        if (topic instanceof String t) {
            builder.withTopic(t);
        }

        Object partition = stored.get(PREFIX + "partition");
        if (partition instanceof Number p) {
            builder.withPartition(p.intValue());
        }

        Object timestamp = stored.get(PREFIX + "timestamp");
        if (timestamp instanceof Number ts) {
            builder.withTimestamp(Instant.ofEpochMilli(ts.longValue()));
        }

        RecordHeaders recordHeaders = new RecordHeaders();
        Object headers = stored.get(PREFIX + "headers");
        if (headers instanceof Map<?, ?> headerMap) {
            for (Map.Entry<?, ?> entry : headerMap.entrySet()) {
                String headerName = String.valueOf(entry.getKey());
                byte[] value = Base64.getDecoder().decode(String.valueOf(entry.getValue()));
                recordHeaders.add(new RecordHeader(headerName, value));
            }
        }
        recordHeaders.remove("outbox-id");
        if (record.getId() != null) {
            recordHeaders.add(new RecordHeader("outbox-id",
                    record.getId().toString().getBytes(StandardCharsets.UTF_8)));
        }
        builder.withHeaders(recordHeaders);

        return Metadata.of(builder.build());
    }

    private static Object parseKey(String value, String typeName) {
        return switch (typeName) {
            case "java.lang.Integer" -> Integer.valueOf(value);
            case "java.lang.Long" -> Long.valueOf(value);
            case "java.lang.Short" -> Short.valueOf(value);
            case "java.lang.Float" -> Float.valueOf(value);
            case "java.lang.Double" -> Double.valueOf(value);
            case "java.util.UUID" -> UUID.fromString(value);
            case BYTES_KEY_TYPE -> Base64.getDecoder().decode(value);
            default -> value;
        };
    }
}
