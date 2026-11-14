package io.quarkiverse.messaging.outbox.it.reactive;

import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "customer")
public class ReactiveCustomer {

    @Id
    private UUID id;

    private String name;

    public ReactiveCustomer() {
    }

    public ReactiveCustomer(String name) {
        this.id = UUID.randomUUID();
        this.name = name;
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
