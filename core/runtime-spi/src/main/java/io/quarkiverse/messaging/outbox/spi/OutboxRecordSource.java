package io.quarkiverse.messaging.outbox.spi;

import java.time.Instant;
import java.util.List;

import io.smallrye.mutiny.Uni;

/**
 * Supplies committed outbox records to the relay, and records what happened to them.
 * <p>
 * The relay does the rest: ordering, retries, mapping records to messages and handling acks and nacks.
 * A source only decides where records come from and how their progress is stored. The default source
 * polls the outbox table through the {@link OutboxStore}. Another one could read the database's
 * transaction log instead.
 *
 * <h2>Threads</h2>
 * Every method is called on the relay's context, and the source must call the {@link OutboxRecordSink} and the
 * {@link Listener} on that context too. {@link OutboxRecordSourceContext#getExecutor()} runs an action there, and
 * {@link OutboxRecordSourceContext#schedule} and {@link OutboxRecordSourceContext#schedulePeriodically} run timers
 * there. The {@link Uni}s returned by the source may complete on any thread: the relay moves back to its context.
 *
 * <h2>Lifecycle</h2>
 * {@link #start} is called when the channel is subscribed, {@link #stop} then {@link #close} when it is cancelled
 * or the application stops. After {@link Listener#failed}, the relay calls {@link #stop} and {@link #close}, and
 * {@link #start} again later: a source must support being started again after it was closed.
 *
 * <h2>Records handed back</h2>
 * The relay hands records back through {@link #released} in three cases:
 * <ul>
 * <li>{@code ordering=none}: a nacked record, with {@link OutboxRecord#getNextAttemptAt()} set to when it may be
 * retried. The source hands it out again once that time has come, like a new record. The polling source only
 * claims records that are due.</li>
 * <li>{@code ordering=key}: the records queued behind a nacked one, see below.</li>
 * <li>On shutdown, through {@link #close}: the records the relay did not send.</li>
 * </ul>
 *
 * <h2>Ordering by key</h2>
 * Under {@code ordering=key}, when a record is nacked, the relay keeps it to retry it, and hands the records it
 * queued behind it with the same key back through {@link #released}. From then on, while the key is in
 * {@link OutboxRecordSink#busyGroups()}, the source must not hand the relay any record of that key: neither the
 * released ones nor later ones. Otherwise they would overtake the record being retried. Once the key is no longer
 * busy, the released records come first, then the later ones, in their original order.
 * <p>
 * A source that reads records on demand can simply leave them out of its reads while their key is busy; the
 * polling source excludes busy keys from its claims. A source that reads a stream must hold them back itself.
 * Handing them back frees the relay's capacity, so that one failing key cannot fill the relay and stop the
 * others.
 * <p>
 * Sources are created by a {@link Factory} bean, selected with {@code @Identifier} and the channel's
 * {@code outbox.relay.source} property.
 */
public interface OutboxRecordSource {

    /**
     * Starts handing records to the sink.
     *
     * @param listener told when the source can no longer read the channel
     */
    void start(OutboxRecordSink sink, Listener listener);

    /** A transaction that wrote a record on this channel has committed. A hint: a source may ignore it. */
    default void signal() {
    }

    /** The sink has capacity again. */
    void capacityAvailable();

    /** The broker acknowledged the record. */
    Uni<Void> delivered(OutboxRecord record);

    /** A send failed. The relay will try again at {@code nextAttempt}. */
    Uni<Void> failed(OutboxRecord record, Instant nextAttempt, Throwable failure);

    /** The relay gave up on the record. */
    Uni<Void> parked(OutboxRecord record, Throwable failure);

    /**
     * The relay hands these records back unsent, in order. See <em>Records handed back</em> above for when they
     * may be handed out again.
     */
    void released(List<OutboxRecord> records);

    /**
     * Stops reading new records. Outcomes of records already handed out may still be reported, until
     * {@link #close(List)} is called.
     */
    void stop();

    /** Takes back the records the relay did not send, and gives up any exclusive access. Never fails. */
    Uni<Void> close(List<OutboxRecord> unsent);

    /** Told about events that make the relay drop the records it holds. */
    interface Listener {

        /**
         * The source can no longer read this channel, for example because another instance took it over.
         * The records it handed out now belong to someone else. The source keeps running, and may get access
         * back later.
         */
        void lostAccess();

        /**
         * The source cannot go on, for example because its connection to the database's log was lost for good.
         * <p>
         * The relay logs the failure, reports the channel as down in the health check, and drops the records it
         * holds. It then calls {@link OutboxRecordSource#stop()} and {@link OutboxRecordSource#close(List)}, and
         * starts the source again after a delay that grows from {@code backoff.initial} to {@code backoff.max}.
         * The channel is reported up again when the source is restarted.
         */
        void failed(Throwable failure);
    }

    /** Creates the source of one channel. Implementations are beans qualified with {@code @Identifier}. */
    interface Factory {

        OutboxRecordSource create(OutboxRecordSourceContext context);
    }
}
