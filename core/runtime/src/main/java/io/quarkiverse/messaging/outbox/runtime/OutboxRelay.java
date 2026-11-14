package io.quarkiverse.messaging.outbox.runtime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.jboss.logging.Logger;

import io.quarkiverse.messaging.outbox.runtime.relay.Dispatcher;
import io.quarkiverse.messaging.outbox.runtime.relay.OrderingPolicy;
import io.quarkiverse.messaging.outbox.runtime.relay.PollingRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordMapper;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSource;
import io.quarkiverse.messaging.outbox.spi.OutboxRecordSourceContext;
import io.quarkiverse.messaging.outbox.spi.OutboxStore;
import io.quarkus.vertx.core.runtime.context.VertxContextSafetyToggle;
import io.smallrye.common.vertx.VertxContext;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

/**
 * Moves committed outbox records to the channel's connector. It is the publisher of the outgoing channel.
 * <p>
 * The work is split in two:
 * <ul>
 * <li>an {@link OutboxRecordSource} gets committed records. The default {@link PollingRecordSource} claims them
 * from the outbox table, and holds the channel lease when the channel is ordered;</li>
 * <li>a {@link Dispatcher} sends them downstream and reports each ack or nack back to the source.</li>
 * </ul>
 * What differs between strict, key and no ordering is in {@link OrderingPolicy}.
 * <p>
 * All relay state is touched from one Vert.x context only. This class owns that context and the lifecycle:
 * subscribe, lease loss, cancel and shutdown.
 */
public class OutboxRelay implements Executor {

    private static final Logger log = Logger.getLogger(OutboxRelay.class);

    private final Context context;
    private final Vertx vertx;
    private final String channel;
    private final OutboxConfig config;
    private final OutboxRecordSource source;
    private final Dispatcher dispatcher;
    private final OutboxRecordSource.Listener listener = new OutboxRecordSource.Listener() {
        @Override
        public void lostAccess() {
            onLostAccess();
        }

        @Override
        public void failed(Throwable failure) {
            onSourceFailed(failure);
        }
    };

    // --- On the relay's context ---
    /** Subscribed and not cancelled or shut down. */
    private boolean running;
    private int restarts;
    private Cancellable restartTimer;
    /** Why the source stopped, until it is started again. Read by the health check. */
    private volatile Throwable sourceFailure;

    /** A relay that polls the outbox table. */
    public OutboxRelay(OutboxStore store, OutboxRecordMapper mapper, List<OutboxMetadataConverter> metadataConverters,
            OutboxConfig config, Clock clock, String channel, Vertx vertx) {
        this(store, mapper, metadataConverters, config, clock, channel, vertx, null, null);
    }

    /**
     * @param sourceFactory creates the record source, or {@code null} for the polling source
     * @param channelConfig the channel's configuration, passed to the source factory; may be {@code null}
     */
    public OutboxRelay(OutboxStore store, OutboxRecordMapper mapper, List<OutboxMetadataConverter> metadataConverters,
            OutboxConfig config, Clock clock, String channel, Vertx vertx, OutboxRecordSource.Factory sourceFactory,
            Config channelConfig) {
        this.context = VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
        VertxContextSafetyToggle.setContextSafe(this.context, true);
        this.vertx = vertx;
        this.channel = channel;
        this.config = config;

        OrderingPolicy policy = OrderingPolicy.of(config.ordering());
        this.source = sourceFactory == null
                ? new PollingRecordSource(store, config, policy, clock, channel, vertx, this)
                : sourceFactory.create(new OutboxRecordSourceContext(channel, channelConfig, store, clock, this,
                        scheduler()));
        this.dispatcher = new Dispatcher(policy, config, mapper, metadataConverters, clock, channel, this,
                (delayMs, action) -> vertx.setTimer(delayMs, id -> execute(action)), source);
    }

    /** Runs the action on the relay's context. */
    @Override
    public void execute(Runnable action) {
        context.runOnContext(v -> action.run());
    }

    /** Vert.x timers whose actions run on the relay's context. */
    private OutboxRecordSourceContext.Scheduler scheduler() {
        return new OutboxRecordSourceContext.Scheduler() {
            @Override
            public Cancellable schedule(Duration delay, Runnable action) {
                long id = vertx.setTimer(Math.max(1, delay.toMillis()), t -> execute(action));
                return () -> vertx.cancelTimer(id);
            }

            @Override
            public Cancellable schedulePeriodically(Duration period, Runnable action) {
                long id = vertx.setPeriodic(Math.max(1, period.toMillis()), t -> execute(action));
                return () -> vertx.cancelTimer(id);
            }
        };
    }

    /** Why the record source stopped, while the relay waits to start it again. */
    public Optional<Throwable> getSourceFailure() {
        return Optional.ofNullable(sourceFailure);
    }

    public String getChannel() {
        return channel;
    }

    public Multi<Message<?>> getPublisher() {
        return Multi.createFrom().emitter(emitter -> {
            execute(() -> {
                running = true;
                dispatcher.start(emitter);
                source.start(dispatcher, listener);
            });
            emitter.onTermination(() -> execute(this::onCancel));
        });
    }

    /** A transaction that wrote a record has committed: look for it now instead of waiting for the poll timer. */
    public void signal() {
        execute(source::signal);
    }

    /**
     * Stops claiming, waits for the messages already sent to be acked or nacked, then gives the remaining
     * records and the lease back. The caller bounds the wait.
     */
    public CompletableFuture<Void> shutdown() {
        CompletableFuture<Void> done = new CompletableFuture<>();
        execute(() -> {
            stopRunning();
            source.stop();
            dispatcher.stop();
            dispatcher.whenIdle(() -> giveBack(done));
        });
        return done;
    }

    public Instant computeNextAttempt(int attempts) {
        return dispatcher.computeNextAttempt(attempts);
    }

    // --- On the relay's context ---

    /** The source can no longer read the channel: forget everything held here. It now belongs to someone else. */
    private void onLostAccess() {
        dispatcher.dropAll();
    }

    private void onCancel() {
        stopRunning();
        source.stop();
        dispatcher.stop();
        giveBack(new CompletableFuture<>());
    }

    private void stopRunning() {
        running = false;
        if (restartTimer != null) {
            restartTimer.cancel();
            restartTimer = null;
        }
    }

    /**
     * The source cannot go on: drop what it handed out, close it, and start it again after a growing delay.
     * The channel is reported down meanwhile.
     */
    private void onSourceFailed(Throwable failure) {
        if (!running || sourceFailure != null) {
            return;
        }
        sourceFailure = failure;
        long delayMs = Math.min(config.backoffMax().toMillis(),
                config.backoffInitial().toMillis() * (1L << Math.min(restarts, 20)));
        restarts++;
        log.errorf(failure, "Outbox record source of channel '%s' failed, restarting it in %d ms", channel, delayMs);
        source.stop();
        source.close(dispatcher.dropAll())
                .emitOn(this)
                .subscribe().with(v -> scheduleRestart(delayMs), e -> scheduleRestart(delayMs));
    }

    private void scheduleRestart(long delayMs) {
        if (!running) {
            return;
        }
        long id = vertx.setTimer(Math.max(1, delayMs), t -> execute(this::restartSource));
        restartTimer = () -> vertx.cancelTimer(id);
    }

    private void restartSource() {
        restartTimer = null;
        if (!running) {
            return;
        }
        log.infof("Outbox restarting the record source of channel '%s'", channel);
        sourceFailure = null;
        source.start(dispatcher, listener);
    }

    private void giveBack(CompletableFuture<Void> done) {
        source.close(dispatcher.dropAll())
                .emitOn(this)
                .subscribe().with(v -> done.complete(null), e -> done.complete(null));
    }
}
