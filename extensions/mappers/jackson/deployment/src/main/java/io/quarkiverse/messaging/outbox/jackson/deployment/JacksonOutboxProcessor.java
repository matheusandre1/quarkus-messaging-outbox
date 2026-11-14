package io.quarkiverse.messaging.outbox.jackson.deployment;

import io.quarkiverse.messaging.outbox.jackson.JacksonOutboxRecordMapper;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class JacksonOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-jackson");
    }

    @BuildStep
    AdditionalBeanBuildItem registerMapper() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(JacksonOutboxRecordMapper.class)
                .setUnremovable()
                .build();
    }
}
