package io.quarkiverse.messaging.outbox.spi;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executor;

import org.eclipse.microprofile.config.Config;

import io.smallrye.mutiny.subscription.Cancellable;

/** What a {@link OutboxRecordSource} gets to work with. */
public class OutboxRecordSourceContext {

    private final String channel;
    private final Config channelConfig;
    private final OutboxStore store;
    private final Clock clock;
    private final Executor executor;
    private final Scheduler scheduler;

    /** Runs actions on the relay's context after a delay. */
    public interface Scheduler {

        Cancellable schedule(Duration delay, Runnable action);

        Cancellable schedulePeriodically(Duration period, Runnable action);
    }

    public OutboxRecordSourceContext(String channel, Config channelConfig, OutboxStore store, Clock clock,
            Executor executor, Scheduler scheduler) {
        this.channel = channel;
        this.channelConfig = channelConfig;
        this.store = store;
        this.clock = clock;
        this.executor = executor;
        this.scheduler = scheduler;
    }

    public String getChannel() {
        return channel;
    }

    /** The outgoing channel's configuration, or {@code null} when the channel has none. */
    public Config getChannelConfig() {
        return channelConfig;
    }

    /** The channel's store, for sources that keep their progress in the outbox table. */
    public OutboxStore getStore() {
        return store;
    }

    public Clock getClock() {
        return clock;
    }

    /** Runs an action on the relay's context. */
    public Executor getExecutor() {
        return executor;
    }

    /** Runs an action once on the relay's context after {@code delay}. */
    public Cancellable schedule(Duration delay, Runnable action) {
        return scheduler.schedule(delay, action);
    }

    /** Runs an action on the relay's context every {@code period}, the first time after one period. */
    public Cancellable schedulePeriodically(Duration period, Runnable action) {
        return scheduler.schedulePeriodically(period, action);
    }
}
