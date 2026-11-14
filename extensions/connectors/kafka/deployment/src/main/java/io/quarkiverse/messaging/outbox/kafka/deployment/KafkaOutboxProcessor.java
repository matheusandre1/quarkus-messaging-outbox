package io.quarkiverse.messaging.outbox.kafka.deployment;

import java.util.List;

import io.quarkiverse.messaging.outbox.deployment.OutboxChannelPayloadBuildItem;
import io.quarkiverse.messaging.outbox.kafka.KafkaOutboxMetadataConverter;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.RunTimeConfigurationDefaultBuildItem;

class KafkaOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-kafka");
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClass(KafkaOutboxMetadataConverter.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    void setKafkaSerializerDefaults(List<OutboxChannelPayloadBuildItem> outboxChannels,
            BuildProducer<RunTimeConfigurationDefaultBuildItem> defaults) {

        if (!isStorePathActive()) {
            return;
        }

        for (OutboxChannelPayloadBuildItem channel : outboxChannels) {
            String prefix = "mp.messaging.outgoing." + channel.getChannel() + ".";
            defaults.produce(new RunTimeConfigurationDefaultBuildItem(
                    prefix + "value.serializer", "io.quarkus.kafka.client.serialization.ObjectMapperSerializer"));
            defaults.produce(new RunTimeConfigurationDefaultBuildItem(
                    prefix + "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        }
    }

    private boolean isStorePathActive() {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        try {
            cl.loadClass("io.quarkiverse.messaging.outbox.hibernate.orm.HibernateOrmOutboxStoreFactory");
            return true;
        } catch (ClassNotFoundException e) {
        }
        try {
            cl.loadClass("io.quarkiverse.messaging.outbox.hibernate.reactive.HibernateReactiveOutboxStoreFactory");
            return true;
        } catch (ClassNotFoundException e) {
        }
        return false;
    }
}
