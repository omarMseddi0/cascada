package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Combines typed partial frames without materializing a row map or a key list for each input row. */
public final class TypedFrameAggregator {
    private static final int EMPTY = -1;
    private static final int INITIAL_CAPACITY = 16;

    private TypedFrameAggregator() { }

    public static ResultFrame aggregate(List<ResultFrame> frames, List<String> dimensions,
                                        Map<String, AggregateFunction> functions, String timeColumn, int step) {
        if (frames.isEmpty()) return ResultFrame.empty();
        validateCompatibleSchemas(frames);
        Accumulator accumulator = new Accumulator(frames.get(0), dimensions, functions, timeColumn, step);
        for (ResultFrame frame : frames) accumulator.accept(frame);
        return accumulator.finish();
    }

    /** Ordered incremental aggregation; completed input frames are not retained by this state. */
    public static final class Accumulator {
        private final ResultFrame schema;
        private final List<String> dimensions;
        private final List<String> measures;
        private final int[] dimensionColumns;
        private final ColumnType[] dimensionTypes;
        private final boolean[] timeDimensions;
        private final int step;
        private final GroupTable groups;

        public Accumulator(ResultFrame inputSchema, List<String> dimensions,
                           Map<String, AggregateFunction> functions, String timeColumn, int step) {
            ResultFrame.Builder descriptor = ResultFrame.builder();
            for (String column : inputSchema.columnNames()) descriptor.column(column, inputSchema.columnType(column));
            this.schema = descriptor.build();
            this.dimensions = List.copyOf(dimensions);
            this.step = step;
            measures = selectMeasures(schema, dimensions, functions);

            dimensionColumns = new int[dimensions.size()];
            dimensionTypes = new ColumnType[dimensions.size()];
            timeDimensions = new boolean[dimensions.size()];
            for (int dimension = 0; dimension < dimensions.size(); dimension++) {
                String name = dimensions.get(dimension);
                dimensionColumns[dimension] = schema.columnNames().indexOf(name);
                if (dimensionColumns[dimension] >= 0) {
                    dimensionTypes[dimension] = schema.columnType(name);
                }
                timeDimensions[dimension] = name.equals(timeColumn);
            }

            int[] measureColumns = new int[measures.size()];
            ColumnType[] measureTypes = new ColumnType[measures.size()];
            AggregateFunction[] measureFunctions = new AggregateFunction[measures.size()];
            for (int measure = 0; measure < measures.size(); measure++) {
                String name = measures.get(measure);
                measureColumns[measure] = schema.columnIndex(name);
                measureTypes[measure] = schema.columnType(name);
                // Name normalization and aggregate resolution belong outside the row/measure hot loop.
                measureFunctions[measure] = AggregateFunctionResolver.resolve(name, functions);
            }

            groups = new GroupTable(schema, dimensionColumns, dimensionTypes, timeDimensions, step,
                    measureColumns, measureTypes, measureFunctions);
        }

        public void accept(ResultFrame input) {
            validateCompatibleSchemas(List.of(schema, input));
            ResultFrame.ColumnReader[] columns = new ResultFrame.ColumnReader[input.columnNames().size()];
            for (int column = 0; column < columns.length; column++) columns[column] = input.columnReader(column);
            for (int row = 0; row < input.rowCount(); row++) {
                int group = groups.findOrCreate(columns, row);
                groups.accumulate(group, columns, row);
            }
        }

        public ResultFrame finish() {
                ResultFrame.Builder result = ResultFrame.builder().expectedRows(groups.groupCount);
            int[] outputDimensions = new int[schema.columnNames().size()];
            int[] outputMeasures = new int[schema.columnNames().size()];
            Arrays.fill(outputDimensions, -1);
            Arrays.fill(outputMeasures, -1);
            for (int column = 0; column < schema.columnNames().size(); column++) {
                String name = schema.columnNames().get(column);
                result.column(name, schema.columnType(name));
                for (int dimension = 0; dimension < dimensions.size(); dimension++) {
                    if (dimensions.get(dimension).equals(name)) {
                        outputDimensions[column] = dimension;
                        break;
                    }
                }
                if (outputDimensions[column] < 0) {
                    for (int measure = 0; measure < measures.size(); measure++) {
                        if (measures.get(measure).equals(name)) {
                            outputMeasures[column] = measure;
                            break;
                        }
                    }
                }
            }

            int[] order = groups.sortedGroupIds(dimensions, dimensionColumns, dimensionTypes, timeDimensions, step);
            for (int group : order) {
                for (int column = 0; column < schema.columnNames().size(); column++) {
                    int dimension = outputDimensions[column];
                    if (dimension >= 0) {
                        groups.appendDimension(result, group, dimension, dimensionTypes[dimension],
                        timeDimensions[dimension]);
                    } else {
                        groups.appendMeasure(result, group, outputMeasures[column]);
                    }
                }
            }
            return result.build();
        }

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

    private static final class GroupTable {
        private final ResultFrame schema;
        private final int[] dimensionColumns;
        private final ColumnType[] dimensionTypes;
        private final boolean[] timeDimensions;
        private final int step;
        private final int[] measureColumns;
        private final ColumnType[] measureTypes;
        private final AggregateFunction[] measureFunctions;

        private int[] slots = new int[INITIAL_CAPACITY];
        private int slotMask = INITIAL_CAPACITY - 1;
        private int resizeThreshold = INITIAL_CAPACITY * 2 / 3;
        private int groupCount;
        private int groupCapacity = INITIAL_CAPACITY;
        private int[] groupHashes = new int[groupCapacity];
        private final boolean[][] keyPresent;
        private final long[][] keyLongs;
        private final double[][] keyDoubles;
        private final String[][] keyStrings;
        private final BigDecimal[][] keyDecimals;
        private final boolean[][] measurePresent;
        private final long[][] longValues;
        private final double[][] doubleValues;
        private final String[][] stringValues;
        private final BigDecimal[][] decimalValues;

        private GroupTable(ResultFrame schema, int[] dimensionColumns, ColumnType[] dimensionTypes,
                           boolean[] timeDimensions, int step, int[] measureColumns,
                           ColumnType[] measureTypes, AggregateFunction[] measureFunctions) {
            this.schema = schema;
            this.dimensionColumns = dimensionColumns;
            this.dimensionTypes = dimensionTypes;
            this.timeDimensions = timeDimensions;
            this.step = step;
            this.measureColumns = measureColumns;
            this.measureTypes = measureTypes;
            this.measureFunctions = measureFunctions;
            int keyCount = dimensionColumns.length;
            keyPresent = new boolean[keyCount][groupCapacity];
            keyLongs = new long[keyCount][];
            keyDoubles = new double[keyCount][];
            keyStrings = new String[keyCount][];
            keyDecimals = new BigDecimal[keyCount][];
            for (int dimension = 0; dimension < keyCount; dimension++) {
                if (dimensionColumns[dimension] < 0) continue;
                if (timeDimensions[dimension]) keyLongs[dimension] = new long[groupCapacity];
                else switch (dimensionTypes[dimension]) {
                    case LONG -> keyLongs[dimension] = new long[groupCapacity];
                    case DOUBLE -> keyDoubles[dimension] = new double[groupCapacity];
                    case STRING -> keyStrings[dimension] = new String[groupCapacity];
                    case DECIMAL -> keyDecimals[dimension] = new BigDecimal[groupCapacity];
                }
            }
            int measures = measureColumns.length;
            measurePresent = new boolean[measures][groupCapacity];
            longValues = new long[measures][];
            doubleValues = new double[measures][];
            stringValues = new String[measures][];
            decimalValues = new BigDecimal[measures][];
            for (int measure = 0; measure < measures; measure++) {
                switch (measureTypes[measure]) {
                    case LONG -> longValues[measure] = new long[groupCapacity];
                    case DOUBLE -> doubleValues[measure] = new double[groupCapacity];
                    case STRING -> stringValues[measure] = new String[groupCapacity];
                    case DECIMAL -> decimalValues[measure] = new BigDecimal[groupCapacity];
                }
            }
            Arrays.fill(slots, EMPTY);
        }

        private int findOrCreate(ResultFrame.ColumnReader[] input, int row) {
            int hash = hashDimensions(input, row);
            int slot = hash & slotMask;
            while (slots[slot] != EMPTY) {
                int group = slots[slot];
                if (groupHashes[group] == hash && sameDimensions(group, input, row)) {
                    return group;
                }
                slot = (slot + 1) & slotMask;
            }
            if (groupCount + 1 > resizeThreshold) {
                growSlots();
                slot = hash & slotMask;
                while (slots[slot] != EMPTY) {
                    slot = (slot + 1) & slotMask;
                }
            }
            ensureGroupCapacity();
            int group = groupCount++;
            groupHashes[group] = hash;
            copyDimensions(group, input, row);
            slots[slot] = group;
            return group;
        }

        private int hashDimensions(ResultFrame.ColumnReader[] input, int row) {
            int hash = 1;
            for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
                int column = dimensionColumns[dimension];
                if (column < 0 || input[column].isNullAt(row)) {
                    hash = 31 * hash;
                    continue;
                }
                int valueHash;
                if (timeDimensions[dimension]) {
                    valueHash = Long.hashCode(timeValue(input, row, dimension));
                } else {
                    valueHash = switch (dimensionTypes[dimension]) {
                        case LONG -> Long.hashCode(input[column].longValue(row));
                        case DOUBLE -> Double.hashCode(input[column].doubleValue(row));
                        case STRING -> input[column].stringValue(row).hashCode();
                        case DECIMAL -> (input[column].decimalValue(row)).stripTrailingZeros().hashCode();
                    };
                }
                hash = 31 * hash + valueHash;
            }
            hash ^= hash >>> 16;
            hash *= 0x7feb352d;
            hash ^= hash >>> 15;
            return hash;
        }

        private void copyDimensions(int group, ResultFrame.ColumnReader[] input, int row) {
            for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
                int column = dimensionColumns[dimension];
                if (column < 0 || input[column].isNullAt(row)) continue;
                keyPresent[dimension][group] = true;
                if (timeDimensions[dimension]) keyLongs[dimension][group] = timeValue(input, row, dimension);
                else switch (dimensionTypes[dimension]) {
                    case LONG -> keyLongs[dimension][group] = input[column].longValue(row);
                    case DOUBLE -> keyDoubles[dimension][group] = input[column].doubleValue(row);
                    case STRING -> keyStrings[dimension][group] = input[column].stringValue(row);
                    case DECIMAL -> keyDecimals[dimension][group] = input[column].decimalValue(row);
                }
            }
        }

        private boolean sameDimensions(int group, ResultFrame.ColumnReader[] input, int row) {
            for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
                int column = dimensionColumns[dimension];
                if (column < 0) continue;
                boolean present = !input[column].isNullAt(row);
                if (present != keyPresent[dimension][group]) return false;
                if (!present) continue;
                if (timeDimensions[dimension]) {
                    if (timeValue(input, row, dimension) != keyLongs[dimension][group]) return false;
                    continue;
                }
                boolean equal = switch (dimensionTypes[dimension]) {
                    case LONG -> input[column].longValue(row) == keyLongs[dimension][group];
                    case DOUBLE -> Double.doubleToLongBits(input[column].doubleValue(row))
                            == Double.doubleToLongBits(keyDoubles[dimension][group]);
                    case STRING -> input[column].stringValue(row).equals(keyStrings[dimension][group]);
                    case DECIMAL -> (input[column].decimalValue(row)).compareTo(keyDecimals[dimension][group]) == 0;
                };
                if (!equal) return false;
            }
            return true;
        }

        private long timeValue(ResultFrame.ColumnReader[] frame, int row, int dimension) {
            int column = dimensionColumns[dimension];
            long timestamp = switch (dimensionTypes[dimension]) {
                case LONG -> frame[column].longValue(row);
                case DOUBLE -> (long) frame[column].doubleValue(row);
                case DECIMAL -> (frame[column].decimalValue(row)).longValue();
                case STRING -> ((Number) (Object) frame[column].stringValue(row)).longValue();
            };
            return Math.floorDiv(timestamp, step) * (long) step;
        }

        private void accumulate(int group, ResultFrame.ColumnReader[] input, int row) {
            for (int measure = 0; measure < measureColumns.length; measure++) {
                int column = measureColumns[measure];
                if (input[column].isNullAt(row)) {
                    continue;
                }
                switch (measureTypes[measure]) {
                    case LONG -> accumulateLong(group, measure, input[column].longValue(row));
                    case DOUBLE -> accumulateDouble(group, measure, input[column].doubleValue(row));
                    case STRING -> accumulateString(group, measure, input[column].stringValue(row));
                    case DECIMAL -> accumulateDecimal(group, measure, input[column].decimalValue(row));
                }
            }
        }

        private void accumulateLong(int group, int measure, long incoming) {
            if (!measurePresent[measure][group]) {
                longValues[measure][group] = incoming;
                measurePresent[measure][group] = true;
                return;
            }
            long previous = longValues[measure][group];
            longValues[measure][group] = switch (measureFunctions[measure]) {
                case SUM, COUNT -> Math.addExact(previous, incoming);
                case MINIMUM -> Math.min(previous, incoming);
                case MAXIMUM -> Math.max(previous, incoming);
            };
        }

        private void accumulateDouble(int group, int measure, double incoming) {
            if (!measurePresent[measure][group]) {
                doubleValues[measure][group] = incoming;
                measurePresent[measure][group] = true;
                return;
            }
            doubleValues[measure][group] = measureFunctions[measure].combine(doubleValues[measure][group], incoming);
        }

        private void accumulateString(int group, int measure, String incoming) {
            if (!measurePresent[measure][group]) {
                stringValues[measure][group] = incoming;
                measurePresent[measure][group] = true;
                return;
            }
            String previous = stringValues[measure][group];
            stringValues[measure][group] = switch (measureFunctions[measure]) {
                case MINIMUM -> compareSparkStrings(previous, incoming) <= 0 ? previous : incoming;
                case MAXIMUM -> compareSparkStrings(previous, incoming) >= 0 ? previous : incoming;
                case SUM, COUNT -> throw new IllegalArgumentException(
                        "STRING aggregate column requires MIN or MAX, got " + measureFunctions[measure]);
            };
        }

        private void accumulateDecimal(int group, int measure, BigDecimal incoming) {
            AggregateFunction function = measureFunctions[measure];
            if (function == AggregateFunction.SUM || function == AggregateFunction.COUNT) {
                requireSparkDecimalPrecision(incoming, schema.columnNames().get(measureColumns[measure]));
            }
            if (!measurePresent[measure][group]) {
                decimalValues[measure][group] = incoming;
                measurePresent[measure][group] = true;
                return;
            }
            BigDecimal previous = decimalValues[measure][group];
            decimalValues[measure][group] = switch (function) {
                case SUM, COUNT -> checkedSparkDecimalSum(previous, incoming,
                        schema.columnNames().get(measureColumns[measure]));
                case MINIMUM -> previous.min(incoming);
                case MAXIMUM -> previous.max(incoming);
            };
        }

        private void ensureGroupCapacity() {
            if (groupCount < groupCapacity) {
                return;
            }
            int grown = nextCapacity(groupCapacity);
            groupHashes = Arrays.copyOf(groupHashes, grown);
            for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
                keyPresent[dimension] = Arrays.copyOf(keyPresent[dimension], grown);
                if (dimensionColumns[dimension] < 0) continue;
                if (timeDimensions[dimension]) keyLongs[dimension] = Arrays.copyOf(keyLongs[dimension], grown);
                else switch (dimensionTypes[dimension]) {
                    case LONG -> keyLongs[dimension] = Arrays.copyOf(keyLongs[dimension], grown);
                    case DOUBLE -> keyDoubles[dimension] = Arrays.copyOf(keyDoubles[dimension], grown);
                    case STRING -> keyStrings[dimension] = Arrays.copyOf(keyStrings[dimension], grown);
                    case DECIMAL -> keyDecimals[dimension] = Arrays.copyOf(keyDecimals[dimension], grown);
                }
            }
            for (int measure = 0; measure < measureColumns.length; measure++) {
                measurePresent[measure] = Arrays.copyOf(measurePresent[measure], grown);
                switch (measureTypes[measure]) {
                    case LONG -> longValues[measure] = Arrays.copyOf(longValues[measure], grown);
                    case DOUBLE -> doubleValues[measure] = Arrays.copyOf(doubleValues[measure], grown);
                    case STRING -> stringValues[measure] = Arrays.copyOf(stringValues[measure], grown);
                    case DECIMAL -> decimalValues[measure] = Arrays.copyOf(decimalValues[measure], grown);
                }
            }
            groupCapacity = grown;
        }

        private void growSlots() {
            if (slots.length > Integer.MAX_VALUE / 2) {
                throw new IllegalStateException("too many distinct groups to merge in one frame");
            }
            slots = new int[slots.length * 2];
            Arrays.fill(slots, EMPTY);
            slotMask = slots.length - 1;
            resizeThreshold = (int) ((long) slots.length * 2 / 3);
            for (int group = 0; group < groupCount; group++) {
                int slot = groupHashes[group] & slotMask;
                while (slots[slot] != EMPTY) {
                    slot = (slot + 1) & slotMask;
                }
                slots[slot] = group;
            }
        }

        private static int nextCapacity(int capacity) {
            if (capacity > Integer.MAX_VALUE / 2) {
                throw new IllegalStateException("too many distinct groups to merge in one frame");
            }
            return capacity * 2;
        }

        private int[] sortedGroupIds(List<String> dimensions, int[] columns, ColumnType[] types,
                                     boolean[] timeDimensions, int step) {
            int[] order = new int[groupCount];
            for (int group = 0; group < groupCount; group++) {
                order[group] = group;
            }
            if (groupCount < 2 || dimensions.isEmpty()) {
                return order;
            }
            int[] scratch = new int[groupCount];
            for (long width = 1; width < groupCount; width *= 2) {
                for (long start = 0; start < groupCount; start += 2 * width) {
                    int left = (int) start;
                    int middle = (int) Math.min(start + width, groupCount);
                    int end = (int) Math.min(start + 2 * width, groupCount);
                    int a = left;
                    int b = middle;
                    int out = left;
                    while (a < middle && b < end) {
                        if (compareGroups(order[a], order[b], dimensions.size(), columns, types,
                                timeDimensions, step) <= 0) {
                            scratch[out++] = order[a++];
                        } else {
                            scratch[out++] = order[b++];
                        }
                    }
                    while (a < middle) scratch[out++] = order[a++];
                    while (b < end) scratch[out++] = order[b++];
                }
                int[] swap = order;
                order = scratch;
                scratch = swap;
            }
            return order;
        }

        private int compareGroups(int left, int right, int dimensionCount, int[] columns,
                                  ColumnType[] types, boolean[] time, int step) {
            for (int dimension = 0; dimension < dimensionCount; dimension++) {
                boolean a = keyPresent[dimension][left], b = keyPresent[dimension][right];
                int comparison;
                if (!a || !b) comparison = a == b ? 0 : a ? 1 : -1;
                else if (time[dimension]) comparison = Long.compare(keyLongs[dimension][left], keyLongs[dimension][right]);
                else comparison = switch (types[dimension]) {
                    case LONG -> Long.compare(keyLongs[dimension][left], keyLongs[dimension][right]);
                    case DOUBLE -> Double.compare(keyDoubles[dimension][left], keyDoubles[dimension][right]);
                    case STRING -> keyStrings[dimension][left].compareTo(keyStrings[dimension][right]);
                    case DECIMAL -> keyDecimals[dimension][left].compareTo(keyDecimals[dimension][right]);
                };
                if (comparison != 0) return comparison;
            }
            return 0;
        }

        private void appendDimension(ResultFrame.Builder output, int group, int dimension,
                                     ColumnType outputType, boolean time) {
            if (!keyPresent[dimension][group]) { output.appendNull(); return; }
            if (time) {
                long value = keyLongs[dimension][group];
                switch (outputType) {
                    case LONG -> output.appendLong(value);
                    case DOUBLE -> output.appendDouble(value);
                    case STRING -> output.appendString(Long.toString(value));
                    case DECIMAL -> throw new IllegalArgumentException("time dimension '"
                            + schema.columnNames().get(dimensionColumns[dimension])
                            + "' cannot be emitted as DECIMAL after bucket flooring");
                }
                return;
            }
            switch (outputType) {
                case LONG -> output.appendLong(keyLongs[dimension][group]);
                case DOUBLE -> output.appendDouble(keyDoubles[dimension][group]);
                case STRING -> output.appendString(keyStrings[dimension][group]);
                case DECIMAL -> output.appendDecimal(keyDecimals[dimension][group]);
            }
        }

        private void appendMeasure(ResultFrame.Builder output, int group, int measure) {
            if (measure < 0 || !measurePresent[measure][group]) {
                output.appendNull();
                return;
            }
            switch (measureTypes[measure]) {
                case LONG -> output.appendLong(longValues[measure][group]);
                case DOUBLE -> output.appendDouble(doubleValues[measure][group]);
                case STRING -> output.appendString(stringValues[measure][group]);
                case DECIMAL -> output.appendDecimal(decimalValues[measure][group]);
            }
        }
    }

    private static java.math.BigDecimal checkedSparkDecimalSum(BigDecimal left, BigDecimal right, String column) {
        BigDecimal sum = left.add(right);
        requireSparkDecimalPrecision(sum, column);
        return sum;
    }

    private static void requireSparkDecimalPrecision(BigDecimal value, String column) {
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
