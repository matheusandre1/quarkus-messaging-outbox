package io.quarkiverse.messaging.outbox.debezium;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.vertx.core.json.JsonObject;

/**
 * Turns a Debezium change event of the outbox record table into an {@link OutboxRecord} to send.
 * <p>
 * A change event is sent when it shows a pending record of the channel:
 * <ul>
 * <li>an insert ({@code c}) or a snapshot read ({@code r}),</li>
 * <li>an update ({@code u}) that leaves the record pending. The Debezium source writes nothing to pending rows,
 * so such an update is an operator setting a parked record back to pending.</li>
 * </ul>
 * Deletes, and updates to sent or parked records, are the source's own outcome writes and are skipped.
 * <p>
 * Values follow Debezium's defaults: {@code bytea} as base64, {@code timestamptz} as an ISO-8601 string. A UUID
 * stored as {@code binary(16)} and timestamps as epoch microseconds are also accepted.
 */
public final class ChangeEventDecoder {

    private final String channel;

    public ChangeEventDecoder(String channel) {
        this.channel = channel;
    }

    public Optional<OutboxRecord> decode(String value) {
        if (value == null) {
            return Optional.empty(); // tombstone
        }
        JsonObject event = new JsonObject(value);
        if (event.containsKey("payload") && event.containsKey("schema")) {
            event = event.getJsonObject("payload"); // converter with schemas enabled
        }
        String op = event.getString("op");
        JsonObject after = event.getJsonObject("after");
        if (after == null || !("c".equals(op) || "r".equals(op) || "u".equals(op))) {
            return Optional.empty();
        }
        if (!channel.equals(after.getString("channel"))
                || after.getInteger("status", -1) != OutboxRecord.STATUS_PENDING) {
            return Optional.empty();
        }
        OutboxRecord record = new OutboxRecord();
        record.setSeq(after.getLong("seq"));
        record.setId(uuid(after.getValue("outbox_id")));
        record.setChannel(channel);
        record.setMsgKey(after.getString("msg_key"));
        record.setPayload(bytes(after.getValue("payload")));
        record.setPayloadType(after.getString("payload_type"));
        String headers = after.getString("headers");
        record.setHeaders(headers == null ? new HashMap<>() : new JsonObject(headers).getMap());
        record.setStatus(OutboxRecord.STATUS_PENDING);
        record.setAttempts(after.getInteger("attempts", 0));
        record.setNextAttemptAt(instant(after.getValue("next_attempt_at")));
        record.setCreatedAt(instant(after.getValue("created_at")));
        record.setLastError(after.getString("last_error"));
        return Optional.of(record);
    }

    static UUID uuid(Object value) {
        if (value == null) {
            return null;
        }
        String text = value.toString();
        if (text.length() == 36) {
            return UUID.fromString(text);
        }
        ByteBuffer bytes = ByteBuffer.wrap(Base64.getDecoder().decode(text)); // binary(16)
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    static byte[] bytes(Object value) {
        if (value == null) {
            return null;
        }
        return value instanceof byte[] b ? b : Base64.getDecoder().decode(value.toString());
    }

    static Instant instant(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number micros) {
            return Instant.EPOCH.plus(micros.longValue(), ChronoUnit.MICROS);
        }
        return OffsetDateTime.parse(value.toString()).toInstant();
    }
}
