package com.cascada.cache.application.service;

import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import com.cascada.cache.application.service.merge.TypedFrameAggregator;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class IncrementalFrameMerge {
    private static final Logger LOGGER = Logger.getLogger(IncrementalFrameMerge.class.getName());
    private final FrameMergeService service;
    private final CanonicalQueryObject canonical;
    ResultFrame schema;
    ResultFrame firstEmpty;
    TypedFrameAccumulator accumulator;
    RuntimeException failure;
    long inputRows;

    IncrementalFrameMerge(FrameMergeService service, CanonicalQueryObject canonical) {
        this.service = service;
        this.canonical = canonical;
    }

    /** Deferred merge errors preserve authoritative fallback after the original fetch/query phases. */
    public void add(ResultFrame frame) {
        if (failure != null) return;
        try {
            java.util.Objects.requireNonNull(frame, "frame");
            if (schema == null) {
                ResultFrameBuilder descriptor = ResultFrame.builder();
                for (String column : frame.columnNames()) descriptor.column(column, frame.columnType(column));
                schema = descriptor.build();
                if (frame.isEmpty()) firstEmpty = frame;
            } else TypedFrameAggregator.validateCompatibleSchemas(List.of(schema, frame));
            inputRows += frame.rowCount();
            if (frame.isEmpty()) return;
            if (accumulator == null) accumulator = service.createAccumulator(schema, canonical);
            accumulator.accept(frame);
        } catch (RuntimeException invalidMerge) { failure = invalidMerge; }
    }

    public ResultFrame finish() {
        if (failure != null) throw failure;
        if (schema == null) return ResultFrame.empty();
        if (accumulator == null) return service.finishMergedFrame(firstEmpty == null ? schema : firstEmpty, canonical);
        boolean monitor = LOGGER.isLoggable(Level.FINE);
        long started = monitor ? System.nanoTime() : 0L;
        ResultFrame merged = accumulator.finish();
        ResultFrame answer = service.finishMergedFrame(merged, canonical);
        if (monitor) LOGGER.fine("Incremental cache merge: input_rows=" + inputRows
                + " output_rows=" + answer.rowCount() + " finish_ms=" + (System.nanoTime() - started) / 1_000_000.0);
        return answer;
    }
}
