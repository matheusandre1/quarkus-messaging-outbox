package io.quarkiverse.messaging.outbox.store.persistence;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;

@Entity
@Table(name = "outbox_record", indexes = {
        @Index(name = "idx_outbox_claim", columnList = "channel, status, next_attempt_at, seq")
})
public class OutboxRecordEntity {

    /** Length of {@code last_error}. The stores truncate the error message to it. */
    public static final int LAST_ERROR_LENGTH = 1000;

    /**
     * The order records are claimed in, assigned by the database when the row is inserted.
     * An identity column is used because it is portable, and because Hibernate's sequence generators
     * hand out pre-allocated blocks per instance, which would interleave across instances.
     */
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(nullable = false, updatable = false)
    private Long seq;

    /** The outbox id, sent to the broker so that consumers can deduplicate. */
    @Column(name = "outbox_id", nullable = false, updatable = false, unique = true)
    private UUID outboxId;

    @Column(nullable = false, updatable = false)
    private String channel;

    @Column(name = "msg_key")
    private String msgKey;

    @Column(length = Integer.MAX_VALUE)
    private byte[] payload;

    @Column(name = "payload_type")
    private String payloadType;

    @Convert(converter = JsonMapConverter.class)
    @Column(length = Integer.MAX_VALUE)
    private Map<String, Object> headers;

    @Column(nullable = false)
    private int status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "claimed_by")
    private String claimedBy;

    @Column(name = "claimed_until")
    private Instant claimedUntil;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "last_error", length = LAST_ERROR_LENGTH)
    private String lastError;

    public UUID getOutboxId() {
        return outboxId;
    }

    public void setOutboxId(UUID outboxId) {
        this.outboxId = outboxId;
    }

    /** The sequence number, or 0 before the row is inserted. */
    public long getSeq() {
        return seq == null ? 0 : seq;
    }

    public void setSeq(Long seq) {
        this.seq = seq;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public void setNextAttemptAt(Instant nextAttemptAt) {
        this.nextAttemptAt = nextAttemptAt;
    }

    public String getClaimedBy() {
        return claimedBy;
    }

    public void setClaimedBy(String claimedBy) {
        this.claimedBy = claimedBy;
    }

    public Instant getClaimedUntil() {
        return claimedUntil;
    }

    public void setClaimedUntil(Instant claimedUntil) {
        this.claimedUntil = claimedUntil;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }
}
