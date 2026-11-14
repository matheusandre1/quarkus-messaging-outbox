package io.quarkiverse.messaging.outbox.spi;

import org.eclipse.microprofile.config.Config;

public class OutboxStoreContext {

    private final String channel;
    private final Config channelConfig;
    private final String relayInstanceId;

    public OutboxStoreContext(String channel, Config channelConfig, String relayInstanceId) {
        this.channel = channel;
        this.channelConfig = channelConfig;
        this.relayInstanceId = relayInstanceId;
    }

    public String getChannel() {
        return channel;
    }

    public Config getChannelConfig() {
        return channelConfig;
    }

    public String getRelayInstanceId() {
        return relayInstanceId;
    }

}
