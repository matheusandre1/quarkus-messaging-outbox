package io.quarkiverse.messaging.outbox.deployment;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarkus.messaging-outbox")
@ConfigRoot(phase = ConfigPhase.BUILD_TIME)
public interface OutboxBuildTimeConfig {

    /**
     * The name of the outbox record table.
     */
    @WithDefault("outbox_record")
    String tableName();

    /**
     * The name of the outbox lease table.
     */
    @WithDefault("outbox_lease")
    String leaseTableName();

    /**
     * The name of the persistence unit to use for outbox entities.
     */
    @WithDefault("<default>")
    String persistenceUnit();
}
