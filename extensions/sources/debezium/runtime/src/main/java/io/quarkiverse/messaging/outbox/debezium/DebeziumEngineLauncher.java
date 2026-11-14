package io.quarkiverse.messaging.outbox.debezium;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.jboss.logging.Logger;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/** Runs the Debezium embedded engine with JSON change events. */
public class DebeziumEngineLauncher implements EngineLauncher {

    private static final Logger log = Logger.getLogger(DebeziumEngineLauncher.class);

    @Override
    public Closeable launch(Properties properties, BatchHandler handler, Consumer<Throwable> onFailure) {
        String name = properties.getProperty("name", "outbox");
        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .using((success, message, error) -> {
                    if (!success) {
                        log.errorf(error, "Debezium engine '%s' stopped: %s", name, message);
                        onFailure.accept(error != null ? error : new IllegalStateException(message));
                    } else {
                        log.debugf("Debezium engine '%s' stopped: %s", name, message);
                    }
                })
                .notifying((records, committer) -> {
                    List<String> values = new ArrayList<>(records.size());
                    for (ChangeEvent<String, String> record : records) {
                        values.add(record.value());
                    }
                    // Throws when the batch is abandoned: nothing is marked, so the engine reads it again.
                    handler.handle(values);
                    for (ChangeEvent<String, String> record : records) {
                        committer.markProcessed(record);
                    }
                    committer.markBatchFinished();
                })
                .build();
        ExecutorService thread = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "outbox-debezium-" + name);
            t.setDaemon(true);
            return t;
        });
        thread.execute(engine);
        return () -> {
            try {
                engine.close();
            } catch (IOException e) {
                log.warnf(e, "Could not close Debezium engine '%s'", name);
            } finally {
                thread.shutdown();
            }
        };
    }
}
