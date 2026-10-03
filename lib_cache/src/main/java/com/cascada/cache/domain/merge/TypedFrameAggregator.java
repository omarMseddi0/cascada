package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Combines typed partial aggregates while preserving their declared dimension and measure types. */
public final class TypedFrameAggregator {
    private TypedFrameAggregator() { }

    public static ResultFrame aggregate(List<ResultFrame> frames, List<String> dimensions,
                                        Map<String, AggregateFunction> functions, String timeColumn, int step) {
        if (frames.isEmpty()) {
            return ResultFrame.empty();
        }
        validateCompatibleSchemas(frames);
        ResultFrame schema = frames.get(0);
        List<String> measures = selectMeasures(schema, dimensions, functions);
        Map<List<Object>, Map<String, Object>> groups = new LinkedHashMap<>();
        for (ResultFrame inputFrame : frames) {
            for (Map<String, Object> inputRow : inputFrame.rows()) {
                List<Object> key = new ArrayList<>();
                List<Object> dimensionValues = new ArrayList<>();
                for (String dimension : dimensions) {
                    Object value = inputRow.get(dimension);
                    if (dimension.equals(timeColumn) && value != null) {
                        value = Math.floorDiv(((Number) value).longValue(), step) * (long) step;
                    }
                    dimensionValues.add(value);
                    key.add(groupingValue(value));
                }
                Map<String, Object> combined = groups.computeIfAbsent(key, ignored -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    for (int dimensionIndex = 0; dimensionIndex < dimensions.size(); dimensionIndex++) {
                        result.put(dimensions.get(dimensionIndex), dimensionValues.get(dimensionIndex));
                    }
                    return result;
                });
                for (String measure : measures) {
                    Object value = inputRow.get(measure);
                    if (value == null) continue;
                    Object previous = combined.get(measure);
                    AggregateFunction function = AggregateFunctionResolver.resolve(measure, functions);
                    if (schema.columnType(measure) == ColumnType.DECIMAL
                            && (function == AggregateFunction.SUM || function == AggregateFunction.COUNT)) {
                        requireSparkDecimalPrecision((java.math.BigDecimal) value, measure);
                    }
                    combined.put(measure, previous == null ? value
                            : combine(previous, value, schema.columnType(measure), function, measure));
                }
            }
        }
        ResultFrame.Builder result = ResultFrame.builder();
        for (String column : schema.columnNames()) {
            if (dimensions.contains(column) || measures.contains(column)) {
                result.column(column, schema.columnType(column));
            }
        }
        List<Map<String, Object>> rows = new ArrayList<>(groups.values());
        rows.sort((leftRow, rightRow) -> {
            for (String dimension : dimensions) {
                int order = compare(leftRow.get(dimension), rightRow.get(dimension));
                if (order != 0) return order;
            }
            return 0;
        });
        rows.forEach(result::row);
        return result.build();
    }

    private static List<String> selectMeasures(ResultFrame schema, List<String> dimensions,
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

    /**
     * Reject incompatible cached partials before reading values. The schema is part of the merge
     * contract: silently treating a missing or differently typed measure as NULL loses data.
     */
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

    private static AggregateFunction declaredFunction(String column,
                                                       Map<String, AggregateFunction> functions) {
        String normalizedColumn = normalize(column);
        for (Map.Entry<String, AggregateFunction> entry : functions.entrySet()) {
            if (normalize(entry.getKey()).equals(normalizedColumn)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static String normalize(String column) {
        return column.replace("`", "").replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
    }

    private static Object groupingValue(Object value) {
        return value instanceof java.math.BigDecimal decimal ? decimal.stripTrailingZeros() : value;
    }

    private static Object combine(Object leftValue, Object rightValue, ColumnType type, AggregateFunction function,
                                  String column) {
        if (type == ColumnType.STRING) {
            String leftString = (String) leftValue;
            String rightString = (String) rightValue;
            return switch (function) {
                case MINIMUM -> compareSparkStrings(leftString, rightString) <= 0 ? leftString : rightString;
                case MAXIMUM -> compareSparkStrings(leftString, rightString) >= 0 ? leftString : rightString;
                case SUM, COUNT -> throw new IllegalArgumentException(
                        "STRING aggregate column requires MIN or MAX, got " + function);
            };
        }
        if (type == ColumnType.DECIMAL) {
            java.math.BigDecimal leftDecimal = (java.math.BigDecimal) leftValue;
            java.math.BigDecimal rightDecimal = (java.math.BigDecimal) rightValue;
            return switch (function) {
                case SUM, COUNT -> checkedSparkDecimalSum(leftDecimal, rightDecimal, column);
                case MINIMUM -> leftDecimal.min(rightDecimal);
                case MAXIMUM -> leftDecimal.max(rightDecimal);
            };
        }
        if (type == ColumnType.LONG) {
            long leftLong = ((Number) leftValue).longValue();
            long rightLong = ((Number) rightValue).longValue();
            return switch (function) {
                // The merge has no ANSI-mode metadata. Refuse overflow so execution falls back to
                // Spark, which applies the session's configured ANSI or legacy behavior.
                case SUM, COUNT -> Math.addExact(leftLong, rightLong);
                case MINIMUM -> Math.min(leftLong, rightLong);
                case MAXIMUM -> Math.max(leftLong, rightLong);
            };
        }
        return function.combine(((Number) leftValue).doubleValue(), ((Number) rightValue).doubleValue());
    }

    private static java.math.BigDecimal checkedSparkDecimalSum(java.math.BigDecimal left,
                                                                java.math.BigDecimal right,
                                                                String column) {
        java.math.BigDecimal sum = left.add(right);
        requireSparkDecimalPrecision(sum, column);
        return sum;
    }

    private static void requireSparkDecimalPrecision(java.math.BigDecimal value, String column) {
        long sparkPrecision = (long) value.precision() - Math.min(0L, (long) value.scale());
        if (sparkPrecision > 38) {
            throw new IllegalArgumentException("cannot merge DECIMAL SUM for column '" + column
                    + "': result precision " + sparkPrecision
                    + " exceeds Spark's maximum precision of 38");
        }
    }

    /** Spark's binary UTF-8 ordering is code-point order for valid Unicode strings. */
    private static int compareSparkStrings(String leftString, String rightString) {
        int leftOffset = 0;
        int rightOffset = 0;
        while (leftOffset < leftString.length() && rightOffset < rightString.length()) {
            int leftCodePoint = leftString.codePointAt(leftOffset);
            int rightCodePoint = rightString.codePointAt(rightOffset);
            int compared = Integer.compare(leftCodePoint, rightCodePoint);
            if (compared != 0) {
                return compared;
            }
            leftOffset += Character.charCount(leftCodePoint);
            rightOffset += Character.charCount(rightCodePoint);
        }
        return Integer.compare(leftString.length() - leftOffset, rightString.length() - rightOffset);
    }

    @SuppressWarnings("unchecked")
    public static int compare(Object left, Object right) {
        if (left == right) return 0;
        if (left == null) return -1;
        if (right == null) return 1;
        return ((Comparable<Object>) left).compareTo(right);
    }
}
