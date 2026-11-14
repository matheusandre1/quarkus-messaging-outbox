package io.quarkiverse.messaging.outbox.runtime;

import io.quarkus.arc.runtime.BeanContainer;
import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class OutboxChannelTypesRecorder {

    public void register(BeanContainer container, String channel, String payloadTypeName) {
        OutboxChannelTypes types = container.beanInstance(OutboxChannelTypes.class);
        types.register(channel, payloadTypeName);
    }
}
