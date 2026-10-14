package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static com.cascada.cache.application.service.merge.TypedFrameAggregator.*;

final class TypedAggregationGroups {
    private final ResultFrame schema;
    private final int[] dimensionColumns;
    private final ColumnType[] dimensionTypes;
    private final boolean[] timeDimensions;
    private final int step;
    private final int[] measureColumns;
    private final ColumnType[] measureTypes;
    private final AggregateOperation[] measureFunctions;

    private int[] slots = new int[INITIAL_CAPACITY];
    private int slotMask = INITIAL_CAPACITY - 1;
    private int resizeThreshold = INITIAL_CAPACITY * 2 / 3;
    int groupCount;
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

    TypedAggregationGroups(ResultFrame schema, int[] dimensionColumns, ColumnType[] dimensionTypes,
                       boolean[] timeDimensions, int step, int[] measureColumns,
                       ColumnType[] measureTypes, AggregateOperation[] measureFunctions) {
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

    int findOrCreate(FrameColumnReader[] input, int row) {
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

    int hashDimensions(FrameColumnReader[] input, int row) {
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

    void copyDimensions(int group, FrameColumnReader[] input, int row) {
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

    boolean sameDimensions(int group, FrameColumnReader[] input, int row) {
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

    long timeValue(FrameColumnReader[] frame, int row, int dimension) {
        int column = dimensionColumns[dimension];
        long timestamp = switch (dimensionTypes[dimension]) {
            case LONG -> frame[column].longValue(row);
            case DOUBLE -> (long) frame[column].doubleValue(row);
            case DECIMAL -> (frame[column].decimalValue(row)).longValue();
            // Numeric epoch text is accepted; other text fails the merge and the engine answers directly.
            case STRING -> Long.parseLong(frame[column].stringValue(row).trim());
        };
        return Math.floorDiv(timestamp, step) * (long) step;
    }

    void accumulate(int group, FrameColumnReader[] input, int row) {
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

    void accumulateLong(int group, int measure, long incoming) {
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

    void accumulateDouble(int group, int measure, double incoming) {
        if (!measurePresent[measure][group]) {
            doubleValues[measure][group] = incoming;
            measurePresent[measure][group] = true;
            return;
        }
        doubleValues[measure][group] = measureFunctions[measure].combine(doubleValues[measure][group], incoming);
    }

    void accumulateString(int group, int measure, String incoming) {
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

    void accumulateDecimal(int group, int measure, BigDecimal incoming) {
        AggregateOperation function = measureFunctions[measure];
        if (function == AggregateOperation.SUM || function == AggregateOperation.COUNT) {
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

    void ensureGroupCapacity() {
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

    void growSlots() {
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

    static int nextCapacity(int capacity) {
        if (capacity > Integer.MAX_VALUE / 2) {
            throw new IllegalStateException("too many distinct groups to merge in one frame");
        }
        return capacity * 2;
    }

    int[] sortedGroupIds(List<String> dimensions, int[] columns, ColumnType[] types,
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

    int compareGroups(int left, int right, int dimensionCount, int[] columns,
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

    void appendDimension(ResultFrameBuilder output, int group, int dimension,
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

    void appendMeasure(ResultFrameBuilder output, int group, int measure) {
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
