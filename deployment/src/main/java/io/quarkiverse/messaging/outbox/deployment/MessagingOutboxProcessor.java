package io.quarkiverse.messaging.outbox.deployment;

import java.util.List;

import org.jboss.jandex.DotName;
import org.jboss.jandex.ParameterizedType;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.runtime.OutboxChannelTypes;
import io.quarkiverse.messaging.outbox.runtime.OutboxChannelTypesRecorder;
import io.quarkiverse.messaging.outbox.runtime.OutboxEmitterFactory;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanContainerBuildItem;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.processor.InjectionPointInfo;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.annotations.ExecutionTime;
import io.quarkus.deployment.annotations.Record;
import io.quarkus.deployment.builditem.FeatureBuildItem;

class MessagingOutboxProcessor {

    private static final String FEATURE = "messaging-outbox";
    private static final DotName OUTBOX_EMITTER = DotName.createSimple(OutboxEmitter.class);
    private static final DotName CHANNEL = DotName.createSimple("org.eclipse.microprofile.reactive.messaging.Channel");

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem(FEATURE);
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(OutboxEmitterFactory.class, OutboxChannelTypes.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    void validateAndCapturePayloadTypes(
            BeanDiscoveryFinishedBuildItem beanDiscovery,
            BuildProducer<OutboxChannelPayloadBuildItem> payloadProducer) {

        for (InjectionPointInfo ip : beanDiscovery.getInjectionPoints()) {
            org.jboss.jandex.Type type = ip.getRequiredType();
            if (!type.name().equals(OUTBOX_EMITTER)) {
                continue;
            }

            if (type.kind() != org.jboss.jandex.Type.Kind.PARAMETERIZED_TYPE) {
                throw new IllegalStateException(
                        "Raw OutboxEmitter injection is not supported. Use OutboxEmitter<T> with a payload type at: "
                                + ip.getTargetInfo());
            }

            ParameterizedType paramType = type.asParameterizedType();
            org.jboss.jandex.Type payloadJandexType = paramType.arguments().get(0);

            String channelName = null;
            for (var qualifier : ip.getRequiredQualifiers()) {
                if (qualifier.name().equals(CHANNEL)) {
                    channelName = qualifier.value().asString();
                    break;
                }
            }
            if (channelName == null) {
                continue;
            }

            payloadProducer.produce(new OutboxChannelPayloadBuildItem(channelName, payloadJandexType.name().toString()));
        }
    }

    @BuildStep
    @Record(ExecutionTime.STATIC_INIT)
    void registerPayloadTypes(
            List<OutboxChannelPayloadBuildItem> payloads,
            OutboxChannelTypesRecorder recorder,
            BeanContainerBuildItem beanContainer) {
        for (OutboxChannelPayloadBuildItem payload : payloads) {
            recorder.register(beanContainer.getValue(), payload.getChannel(), payload.getPayloadTypeName());
        }
    }
}
