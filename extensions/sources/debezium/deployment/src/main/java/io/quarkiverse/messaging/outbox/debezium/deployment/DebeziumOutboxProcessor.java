package io.quarkiverse.messaging.outbox.debezium.deployment;

import io.quarkiverse.messaging.outbox.debezium.DebeziumRecordSourceFactory;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class DebeziumOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-debezium");
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClass(DebeziumRecordSourceFactory.class)
                .setUnremovable()
                .build();
    }
}
