package io.quarkiverse.messaging.outbox.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.vertx.core.json.JsonObject;

class ChangeEventDecoderTest {

    static final UUID ID = UUID.fromString("7f1c2b9e-3d4a-4c5b-9e8f-0a1b2c3d4e5f");

    static JsonObject row(String channel, int status) {
        return new JsonObject()
                .put("seq", 42)
                .put("outbox_id", ID.toString())
                .put("channel", channel)
                .put("msg_key", "customer-1")
                .put("payload", Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.UTF_8)))
                .put("payload_type", "java.lang.String")
                .put("headers", "{\"kafka.key-type\":\"java.lang.String\"}")
                .put("status", status)
                .put("attempts", 0)
                .put("next_attempt_at", null)
                .put("created_at", "2026-11-17T14:00:00.123456Z")
                .put("last_error", null);
    }

    static String event(String op, JsonObject before, JsonObject after) {
        return new JsonObject().put("op", op).put("before", before).put("after", after).encode();
    }

    final ChangeEventDecoder decoder = new ChangeEventDecoder("orders");

    @Test
    void insertOfAPendingRecordIsDecoded() {
        OutboxRecord record = decoder.decode(event("c", null, row("orders", 0))).orElseThrow();
        assertThat(record.getSeq()).isEqualTo(42);
        assertThat(record.getId()).isEqualTo(ID);
        assertThat(record.getChannel()).isEqualTo("orders");
        assertThat(record.getMsgKey()).isEqualTo("customer-1");
        assertThat(new String(record.getPayload(), StandardCharsets.UTF_8)).isEqualTo("hello");
        assertThat(record.getPayloadType()).isEqualTo("java.lang.String");
        assertThat(record.getHeaders()).containsEntry("kafka.key-type", "java.lang.String");
        assertThat(record.getCreatedAt()).isEqualTo(Instant.parse("2026-11-17T14:00:00.123456Z"));
        assertThat(record.getNextAttemptAt()).isNull();
    }

    @Test
    void snapshotReadAndResetToPendingAreDecoded() {
        assertThat(decoder.decode(event("r", null, row("orders", 0)))).isPresent();
        assertThat(decoder.decode(event("u", row("orders", 2), row("orders", 0)))).isPresent();
    }

    @Test
    void outcomesAndOtherChannelsAreSkipped() {
        assertThat(decoder.decode(event("d", row("orders", 0), null))).isEmpty();
        assertThat(decoder.decode(event("u", row("orders", 0), row("orders", 1)))).isEmpty(); // marked sent
        assertThat(decoder.decode(event("u", row("orders", 0), row("orders", 2)))).isEmpty(); // parked
        assertThat(decoder.decode(event("c", null, row("payments", 0)))).isEmpty();
        assertThat(decoder.decode(null)).isEmpty(); // tombstone
    }

    @Test
    void envelopeWithSchemaIsUnwrapped() {
        String withSchema = new JsonObject().put("schema", new JsonObject())
                .put("payload", new JsonObject(event("c", null, row("orders", 0)))).encode();
        assertThat(decoder.decode(withSchema)).isPresent();
    }

    @Test
    void binaryUuidAndMicrosecondTimestampsAreAccepted() {
        byte[] bytes = java.nio.ByteBuffer.allocate(16).putLong(ID.getMostSignificantBits())
                .putLong(ID.getLeastSignificantBits()).array();
        assertThat(ChangeEventDecoder.uuid(Base64.getEncoder().encodeToString(bytes))).isEqualTo(ID);
        assertThat(ChangeEventDecoder.instant(1_700_000_000_123_456L))
                .isEqualTo(Instant.ofEpochSecond(1_700_000_000L, 123_456_000L));
    }
}
