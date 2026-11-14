package io.quarkiverse.messaging.outbox.runtime;

import java.util.Map;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Metadata;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.quarkiverse.messaging.outbox.spi.OutboxMetadataConverter;
import io.quarkiverse.messaging.outbox.spi.OutboxRecord;
import io.smallrye.reactive.messaging.TracingMetadata;

@ApplicationScoped
public class TracingOutboxMetadataConverter implements OutboxMetadataConverter {

    @Override
    public Optional<String> extractKey(Message<?> message) {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> extract(Message<?> message) {
        try {
            Span span = Span.current();
            if (span.getSpanContext().isValid()) {
                var ctx = span.getSpanContext();
                String traceparent = "00-" + ctx.getTraceId() + "-" + ctx.getSpanId() + "-"
                        + ctx.getTraceFlags().asHex();
                return Map.of("traceparent", traceparent);
            }
        } catch (NoClassDefFoundError ignored) {
        }
        return Map.of();
    }

    @Override
    public Metadata restore(OutboxRecord record) {
        try {
            Object traceparent = record.getHeaders() != null ? record.getHeaders().get("traceparent") : null;
            if (traceparent instanceof String tp) {
                String[] parts = tp.split("-");
                if (parts.length == 4) {
                    SpanContext spanContext = SpanContext.createFromRemoteParent(
                            parts[1], parts[2],
                            TraceFlags.fromHex(parts[3], 0),
                            TraceState.getDefault());
                    Context otelContext = Context.current().with(Span.wrap(spanContext));
                    return Metadata.of(TracingMetadata.withPrevious(otelContext));
                }
            }
        } catch (NoClassDefFoundError | Exception ignored) {
        }
        return Metadata.empty();
    }
}
