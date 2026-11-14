package io.quarkiverse.messaging.outbox.sample;

import jakarta.persistence.Entity;

import io.quarkus.hibernate.orm.panache.PanacheEntity;

@Entity
public class Customer extends PanacheEntity {

    public String name;
    public String email;
}
