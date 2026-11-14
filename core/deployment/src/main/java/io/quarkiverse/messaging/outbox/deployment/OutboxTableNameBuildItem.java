package io.quarkiverse.messaging.outbox.deployment;

import io.quarkus.builder.item.SimpleBuildItem;

public final class OutboxTableNameBuildItem extends SimpleBuildItem {

    private final String tableName;
    private final String leaseTableName;
    private final String persistenceUnitName;

    public OutboxTableNameBuildItem(String tableName, String leaseTableName, String persistenceUnitName) {
        this.tableName = tableName;
        this.leaseTableName = leaseTableName;
        this.persistenceUnitName = persistenceUnitName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getLeaseTableName() {
        return leaseTableName;
    }

    public String getPersistenceUnitName() {
        return persistenceUnitName;
    }
}
