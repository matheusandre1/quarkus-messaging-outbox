package io.quarkiverse.messaging.outbox.rabbitmq.deployment;

import io.quarkiverse.messaging.outbox.rabbitmq.RabbitMQOutboxMetadataConverter;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class RabbitMQOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-rabbitmq");
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClass(RabbitMQOutboxMetadataConverter.class)
                .setUnremovable()
                .build();
    }
}
