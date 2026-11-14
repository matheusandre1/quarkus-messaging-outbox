package io.quarkiverse.messaging.outbox.deployment;

import io.quarkus.builder.item.MultiBuildItem;

public final class OutboxChannelPayloadBuildItem extends MultiBuildItem {

    private final String channel;
    private final String payloadTypeName;

    public OutboxChannelPayloadBuildItem(String channel, String payloadTypeName) {
        this.channel = channel;
        this.payloadTypeName = payloadTypeName;
    }

    public String getChannel() {
        return channel;
    }

    public String getPayloadTypeName() {
        return payloadTypeName;
    }
}
