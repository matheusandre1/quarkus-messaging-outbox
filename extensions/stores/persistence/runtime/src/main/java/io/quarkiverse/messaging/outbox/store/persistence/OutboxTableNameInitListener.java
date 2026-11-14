package io.quarkiverse.messaging.outbox.store.persistence;

import java.util.function.BiConsumer;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.mapping.Index;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;

import io.quarkus.hibernate.orm.runtime.spi.HibernateOrmIntegrationStaticInitListener;
import io.quarkus.runtime.annotations.RecordableConstructor;

public class OutboxTableNameInitListener implements HibernateOrmIntegrationStaticInitListener {

    private static final String DEFAULT_RECORD_TABLE = "outbox_record";
    private static final String DEFAULT_LEASE_TABLE = "outbox_lease";
    /** Prefix of the index names declared on the outbox entities, such as idx_outbox_claim. */
    private static final String INDEX_PREFIX = "idx_outbox_";

    private final String recordTableName;
    private final String leaseTableName;

    @RecordableConstructor
    public OutboxTableNameInitListener(String recordTableName, String leaseTableName) {
        this.recordTableName = recordTableName;
        this.leaseTableName = leaseTableName;
    }

    public String getRecordTableName() {
        return recordTableName;
    }

    public String getLeaseTableName() {
        return leaseTableName;
    }

    @Override
    public void contributeBootProperties(BiConsumer<String, Object> propertyCollector) {
    }

    @Override
    public void onMetadataInitialized(Metadata metadata, BootstrapContext bootstrapContext,
            BiConsumer<String, Object> propertyCollector) {
        for (PersistentClass pc : metadata.getEntityBindings()) {
            if (OutboxRecordEntity.class.getName().equals(pc.getClassName())) {
                renameTable(pc.getTable(), DEFAULT_RECORD_TABLE, recordTableName);
            } else if (OutboxLeaseEntity.class.getName().equals(pc.getClassName())) {
                renameTable(pc.getTable(), DEFAULT_LEASE_TABLE, leaseTableName);
            }
        }
    }

    private static void renameTable(Table table, String defaultName, String newName) {
        if (defaultName.equals(newName)) {
            return;
        }
        table.setName(newName);
        for (Index index : table.getIndexes().values()) {
            String idxName = index.getName();
            // Index names are unique per schema on some databases, so they follow the table name.
            if (idxName != null && idxName.startsWith(INDEX_PREFIX)) {
                index.setName("idx_" + newName + idxName.substring(INDEX_PREFIX.length() - 1));
            }
        }
    }
}
