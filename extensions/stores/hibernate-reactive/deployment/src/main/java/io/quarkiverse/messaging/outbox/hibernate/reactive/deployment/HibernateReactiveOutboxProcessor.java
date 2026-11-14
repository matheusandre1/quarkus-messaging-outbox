package io.quarkiverse.messaging.outbox.hibernate.reactive.deployment;

import java.util.Set;

import io.quarkiverse.messaging.outbox.deployment.OutboxTableNameBuildItem;
import io.quarkiverse.messaging.outbox.hibernate.reactive.HibernateReactiveOutboxStoreFactory;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxLeaseEntity;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxRecordEntity;
import io.quarkiverse.messaging.outbox.store.persistence.OutboxTableNameInitListener;
import io.quarkus.arc.deployment.AdditionalBeanBuildItem;
import io.quarkus.deployment.annotations.BuildProducer;
import io.quarkus.deployment.annotations.BuildStep;
import io.quarkus.deployment.builditem.FeatureBuildItem;
import io.quarkus.hibernate.orm.deployment.spi.AdditionalJpaModelBuildItem;
import io.quarkus.hibernate.orm.deployment.spi.HibernateOrmIntegrationStaticConfiguredBuildItem;

class HibernateReactiveOutboxProcessor {

    @BuildStep
    FeatureBuildItem feature() {
        return new FeatureBuildItem("messaging-outbox-hibernate-reactive");
    }

    @BuildStep
    AdditionalBeanBuildItem registerBeans() {
        return AdditionalBeanBuildItem.builder()
                .addBeanClasses(HibernateReactiveOutboxStoreFactory.class)
                .setUnremovable()
                .build();
    }

    @BuildStep
    void contributeJpaModel(OutboxTableNameBuildItem tableNames,
            BuildProducer<AdditionalJpaModelBuildItem> jpaModel) {
        Set<String> pus = Set.of(tableNames.getPersistenceUnitName());
        jpaModel.produce(new AdditionalJpaModelBuildItem(OutboxRecordEntity.class.getName(), pus));
        jpaModel.produce(new AdditionalJpaModelBuildItem(OutboxLeaseEntity.class.getName(), pus));
    }

    @BuildStep
    void configureTableNames(OutboxTableNameBuildItem tableNames,
            BuildProducer<HibernateOrmIntegrationStaticConfiguredBuildItem> integrations) {
        integrations.produce(HibernateOrmIntegrationStaticConfiguredBuildItem
                .builder("messaging-outbox", tableNames.getPersistenceUnitName())
                .initListener(new OutboxTableNameInitListener(tableNames.getTableName(), tableNames.getLeaseTableName()))
                .build());
    }
}
