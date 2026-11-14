-- Quarkus Messaging Outbox schema for MySQL 8.4 and later (InnoDB).
-- Matches what Hibernate ORM generates for the outbox entities, and passes schema validation.
-- With quarkus.messaging-outbox.table-name or lease-table-name set, rename the tables to match.

create table outbox_record (
    seq             bigint not null auto_increment,
    outbox_id       binary(16) not null,
    channel         varchar(255) not null,
    msg_key         varchar(255),
    payload         longblob,
    payload_type    varchar(255),
    headers         longtext,
    status          integer not null,
    attempts        integer not null,
    next_attempt_at datetime(6),
    claimed_by      varchar(255),
    claimed_until   datetime(6),
    created_at      datetime(6) not null,
    last_error      varchar(1000),
    primary key (seq),
    constraint uk_outbox_record_outbox_id unique (outbox_id)
) engine = InnoDB;

create index idx_outbox_claim on outbox_record (channel, status, next_attempt_at, seq);

create table outbox_lease (
    channel    varchar(255) not null,
    owner      varchar(255) not null,
    expires_at datetime(6) not null,
    primary key (channel)
) engine = InnoDB;
