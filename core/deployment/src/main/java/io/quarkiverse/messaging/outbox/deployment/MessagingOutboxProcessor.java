package io.quarkiverse.messaging.outbox.deployment;

import java.util.Set;
import java.util.TreeSet;

import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.IndexView;
import org.jboss.jandex.ParameterizedType;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.quarkiverse.messaging.outbox.runtime.OutboxEmitterFactory;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.arc.deployment.BeanDiscoveryFinishedBuildItem;
import io.quarkus.arc.processor.InjectionPointInfo;
import io.quarkus.deployment.Capabilities;
import io.quarkus.deployment.Capability;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.CombinedIndexBuildItem;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.deployment.builditem.nativeimage.ReflectiveClassBuildItem;

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
                .addBeanClasses(OutboxEmitterFactory.class,
                        io.quarkiverse.messaging.outbox.runtime.TracingOutboxMetadataConverter.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    OutboxTableNameBuildItem tableNames(OutboxBuildTimeConfig config) {
        return new OutboxTableNameBuildItem(config.tableName(), config.leaseTableName(), config.persistenceUnit());
    }

    @BuildStep
    void registerHealthCheck(Capabilities capabilities, BuildProducer<AdditionalBeanBuildItem> beans) {
        if (capabilities.isPresent(Capability.SMALLRYE_HEALTH)) {
            beans.produce(AdditionalBeanBuildItem.builder()
                    .addBeanClasses(io.quarkiverse.messaging.outbox.runtime.observability.OutboxHealthCheck.class)
                    .setUnremovable()
                    .build());
        }
    }

    @BuildStep
    void excludeHealthWhenUnavailable(Capabilities capabilities,
            BuildProducer<io.quarkus.arc.deployment.ExcludedTypeBuildItem> exclusions) {
        if (!capabilities.isPresent(Capability.SMALLRYE_HEALTH)) {
            exclusions.produce(new io.quarkus.arc.deployment.ExcludedTypeBuildItem(
                    io.quarkiverse.messaging.outbox.runtime.observability.OutboxHealthCheck.class.getName()));
        }
    }

    @BuildStep
    void validateAndDiscoverOutboxChannels(
            BeanDiscoveryFinishedBuildItem beanDiscovery,
            CombinedIndexBuildItem combinedIndex,
            BuildProducer<OutboxChannelPayloadBuildItem> channelProducer,
            BuildProducer<ReflectiveClassBuildItem> reflection,
            BuildProducer<io.quarkus.deployment.builditem.ServiceStartBuildItem> serviceStart) {

        Set<String> payloadClasses = new TreeSet<>();

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

            ParameterizedType paramType = type.asParameterizedType();
            org.jboss.jandex.Type payloadType = paramType.arguments().get(0);
            channelProducer.produce(new OutboxChannelPayloadBuildItem(channelName, payloadType.name().toString()));
            collectPayloadClasses(payloadType, combinedIndex.getIndex(), payloadClasses);
        }

        if (!payloadClasses.isEmpty()) {
            // The relay loads the stored payload class by name and the mapper deserializes into it,
            // so both need reflection in a native image.
            reflection.produce(ReflectiveClassBuildItem.builder(payloadClasses)
                    .constructors().methods().fields()
                    .reason(getClass().getName())
                    .build());
        }
    }

    /**
     * Adds the classes a payload of this type can be at runtime: the type itself, its type arguments, and the
     * known subclasses and implementations, since the stored type is the runtime class of the payload.
     */
    static void collectPayloadClasses(org.jboss.jandex.Type type, IndexView index, Set<String> classes) {
        switch (type.kind()) {
            case CLASS -> addWithSubtypes(type.name(), index, classes);
            case PARAMETERIZED_TYPE -> {
                addWithSubtypes(type.name(), index, classes);
                for (org.jboss.jandex.Type argument : type.asParameterizedType().arguments()) {
                    collectPayloadClasses(argument, index, classes);
                }
            }
            case ARRAY -> collectPayloadClasses(type.asArrayType().constituent(), index, classes);
            case WILDCARD_TYPE -> {
                org.jboss.jandex.Type bound = type.asWildcardType().extendsBound();
                if (bound != null) {
                    collectPayloadClasses(bound, index, classes);
                }
            }
            default -> {
                // Primitives, type variables and void carry no class to register.
            }
        }
    }

    private static void addWithSubtypes(DotName name, IndexView index, Set<String> classes) {
        String className = name.toString();
        if (className.startsWith("java.")) {
            return; // JDK types such as String and collections are handled by the JDK and Jackson support
        }
        classes.add(className);
        for (ClassInfo subclass : index.getAllKnownSubclasses(name)) {
            classes.add(subclass.name().toString());
        }
        for (ClassInfo implementor : index.getAllKnownImplementors(name)) {
            classes.add(implementor.name().toString());
        }
    }
}
