package io.quarkiverse.messaging.outbox.debezium;

import java.io.Closeable;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Starts a change data capture engine. The Debezium implementation is {@link DebeziumEngineLauncher}; tests
 * replace it to feed change events without a database.
 */
public interface EngineLauncher {

    /**
     * Starts an engine in a thread of its own.
     *
     * @param onFailure called, on any thread, if the engine stops with an error
     * @return closes the engine
     */
    Closeable launch(Properties properties, BatchHandler handler, Consumer<Throwable> onFailure);

    /** Receives the change events of one batch, as Debezium JSON values. */
    @FunctionalInterface
    interface BatchHandler {

        /**
         * Handles a batch. The engine may record its position past the batch only when this returns normally.
         *
         * @throws InterruptedException when the batch was abandoned: the source stopped or lost the lease
         */
        void handle(List<String> values) throws InterruptedException;
    }
}
