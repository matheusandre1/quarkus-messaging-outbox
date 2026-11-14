package io.quarkiverse.messaging.outbox.store.persistence;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "outbox_lease")
public class OutboxLeaseEntity {

    @Id
    @Column(nullable = false)
    private String channel;

    @Column(nullable = false)
    private String owner;

    @Column(name = "expires_at", nullable = false)
    private Instant expiresAt;

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }
}
