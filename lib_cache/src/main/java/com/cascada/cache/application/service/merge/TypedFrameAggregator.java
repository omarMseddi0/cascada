package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnNames;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Combines typed partial frames without materializing a row map or a key list for each input row. */
public final class TypedFrameAggregator {
    static final int EMPTY = -1;
    static final int INITIAL_CAPACITY = 16;

    private TypedFrameAggregator() { }

    public static ResultFrame aggregate(List<ResultFrame> frames, List<String> dimensions,
                                        Map<String, AggregateFunction> functions, String timeColumn, int step) {
        if (frames.isEmpty()) return ResultFrame.empty();
        validateCompatibleSchemas(frames);
        TypedFrameAccumulator accumulator = new TypedFrameAccumulator(frames.get(0), dimensions, functions, timeColumn, step);
        for (ResultFrame frame : frames) accumulator.accept(frame);
        return accumulator.finish();
    }


    static List<String> selectMeasures(ResultFrame schema, List<String> dimensions,
                                               Map<String, AggregateFunction> functions) {
        List<String> measures = new ArrayList<>();
        for (String column : schema.columnNames()) {
            if (dimensions.contains(column)) {
                continue;
            }
            if (schema.columnType(column) != ColumnType.STRING) {
                measures.add(column);
                continue;
            }
            AggregateFunction declaredFunction = declaredFunction(column, functions);
            if (declaredFunction == AggregateFunction.MINIMUM || declaredFunction == AggregateFunction.MAXIMUM) {
                measures.add(column);
                continue;
            }
            throw new IllegalArgumentException("cannot merge unsupported STRING aggregate column '"
                    + column + "'");
        }
        return measures;
    }

    private static AggregateFunction declaredFunction(String column, Map<String, AggregateFunction> functions) {
        String wanted = ColumnNames.identity(column);
        for (Map.Entry<String, AggregateFunction> entry : functions.entrySet()) {
            if (ColumnNames.identity(entry.getKey()).equals(wanted)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** Reject incompatible cached partials before reading values; schema mismatches lose data. */
    public static void validateCompatibleSchemas(List<ResultFrame> frames) {
        if (frames.isEmpty()) {
            return;
        }
        ResultFrame expected = frames.get(0);
        for (int frameIndex = 1; frameIndex < frames.size(); frameIndex++) {
            ResultFrame actual = frames.get(frameIndex);
            if (!expected.columnNames().equals(actual.columnNames())) {
                throw new IllegalArgumentException("cannot merge frame " + frameIndex
                        + ": column names/order " + actual.columnNames()
                        + " do not match " + expected.columnNames());
            }
            for (String column : expected.columnNames()) {
                ColumnType expectedType = expected.columnType(column);
                ColumnType actualType = actual.columnType(column);
                if (expectedType != actualType) {
                    throw new IllegalArgumentException("cannot merge frame " + frameIndex
                            + ": column '" + column + "' has type " + actualType
                            + ", expected " + expectedType);
                }
            }
        }
    }
}
