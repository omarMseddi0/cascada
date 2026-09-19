package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.*;

/** Combines disjoint partial aggregates without changing dimension types or integer precision. */
public final class TypedFrameAggregator {
    private TypedFrameAggregator() { }

    public static ResultFrame aggregate(List<ResultFrame> frames, List<String> dimensions,
                                        Map<String, AggregateFunction> functions, String timeColumn, int step) {
        ResultFrame schema = frames.get(0);
        List<String> measures = schema.columnNames().stream()
                .filter(c -> !dimensions.contains(c) && schema.columnType(c) != ColumnType.STRING).toList();
        Map<List<Object>, Map<String, Object>> groups = new LinkedHashMap<>();
        for (ResultFrame frame : frames) {
            for (Map<String, Object> row : frame.rows()) {
                List<Object> key = new ArrayList<>();
                for (String dimension : dimensions) {
                    Object value = row.get(dimension);
                    if (dimension.equals(timeColumn) && value != null) {
                        value = Math.floorDiv(((Number) value).longValue(), step) * (long) step;
                    }
                    key.add(value);
                }
                Map<String, Object> combined = groups.computeIfAbsent(key, ignored -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    for (int i = 0; i < dimensions.size(); i++) result.put(dimensions.get(i), key.get(i));
                    return result;
                });
                for (String measure : measures) {
                    Object value = row.get(measure);
                    if (value == null) continue;
                    Object previous = combined.get(measure);
                    AggregateFunction function = AggregateFunctionResolver.resolve(measure, functions);
                    combined.put(measure, previous == null ? value
                            : combine((Number) previous, (Number) value, schema.columnType(measure), function));
                }
            }
        }
        ResultFrame.Builder result = ResultFrame.builder();
        dimensions.forEach(c -> result.column(c, schema.columnType(c)));
        measures.forEach(c -> result.column(c, schema.columnType(c)));
        List<Map<String, Object>> rows = new ArrayList<>(groups.values());
        rows.sort((left, right) -> {
            for (String dimension : dimensions) {
                int order = compare(left.get(dimension), right.get(dimension));
                if (order != 0) return order;
            }
            return 0;
        });
        rows.forEach(result::row);
        return result.build();
    }

    private static Number combine(Number left, Number right, ColumnType type, AggregateFunction function) {
        if (type == ColumnType.DECIMAL) {
            java.math.BigDecimal a = (java.math.BigDecimal) left, b = (java.math.BigDecimal) right;
            return switch (function) {
                case SUM, COUNT -> a.add(b);
                case MINIMUM -> a.min(b);
                case MAXIMUM -> a.max(b);
            };
        }
        if (type == ColumnType.LONG) {
            long a = left.longValue(), b = right.longValue();
            return switch (function) {
                case SUM, COUNT -> Math.addExact(a, b);
                case MINIMUM -> Math.min(a, b);
                case MAXIMUM -> Math.max(a, b);
            };
        }
        return function.combine(left.doubleValue(), right.doubleValue());
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object left, Object right) {
        if (left == right) return 0;
        if (left == null) return -1;
        if (right == null) return 1;
        return ((Comparable<Object>) left).compareTo(right);
    }
}
