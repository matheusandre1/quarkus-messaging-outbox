package io.quarkiverse.messaging.outbox.amqp.deployment;

import io.quarkiverse.messaging.outbox.amqp.AmqpOutboxMetadataConverter;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class AmqpOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-amqp");
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClass(AmqpOutboxMetadataConverter.class)
                .setUnremovable()
                .build();
    }
}
