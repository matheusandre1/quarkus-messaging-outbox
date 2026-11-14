package io.quarkiverse.messaging.outbox.runtime;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.inject.Inject;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Channel;

import io.quarkiverse.messaging.outbox.OutboxEmitter;
import io.smallrye.reactive.messaging.ChannelRegistry;
import io.smallrye.reactive.messaging.EmitterConfiguration;
import io.smallrye.reactive.messaging.EmitterFactory;
import io.smallrye.reactive.messaging.MessagePublisherProvider;
import io.smallrye.reactive.messaging.annotations.EmitterFactoryFor;
import io.smallrye.reactive.messaging.providers.extension.ChannelProducer;

@EmitterFactoryFor(OutboxEmitter.class)
@ApplicationScoped
public class OutboxEmitterFactory implements EmitterFactory<OutboxEmitterImpl<Object>> {

    @Inject
    ChannelRegistry channelRegistry;

    private final Map<String, Config> channelConfigs = new ConcurrentHashMap<>();

    @Override
    public OutboxEmitterImpl<Object> createEmitter(EmitterConfiguration configuration, long defaultBufferSize) {
        return create(configuration, defaultBufferSize, null);
    }

    @Override
    public MessagePublisherProvider<?> createEmitter(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        return create(configuration, defaultBufferSize, channelConfig);
    }

    private OutboxEmitterImpl<Object> create(EmitterConfiguration configuration, long defaultBufferSize,
            Config channelConfig) {
        if (channelConfig != null) {
            channelConfigs.put(configuration.name(), channelConfig);
        }
        return new OutboxEmitterImpl<>(configuration.name(), defaultBufferSize);
    }

    public Config getChannelConfig(String channel) {
        return channelConfigs.get(channel);
    }

    @Produces
    @Channel("")
    <T> OutboxEmitter<T> produce(InjectionPoint injectionPoint) {
        String channel = ChannelProducer.getChannelName(injectionPoint);
        return channelRegistry.getEmitter(channel, OutboxEmitter.class);
    }
}
