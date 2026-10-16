package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static com.cascada.cache.application.service.merge.TypedFrameAggregator.*;

/**
 * Open-addressing group table over typed column arrays. Keys and measures are stored one typed array
 * per column ({@code long[]}, {@code double[]}, {@code String[]} or {@code BigDecimal[]}), allocated
 * and grown by {@link ResultFrameBuilder}, so the hot loops read primitives without boxing.
 */
final class TypedAggregationGroups {
    private final ResultFrame schema;
    private final int[] dimensionColumns;
    private final ColumnType[] dimensionTypes;
    private final ColumnType[] keyStorageTypes;
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
    private final Object[] keys;
    private final boolean[][] measurePresent;
    private final Object[] measures;

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
        keyStorageTypes = new ColumnType[keyCount];
        keys = new Object[keyCount];
        for (int dimension = 0; dimension < keyCount; dimension++) {
            if (dimensionColumns[dimension] < 0) continue;
            // A time dimension is floored to an epoch-second bucket start and kept as a LONG.
            keyStorageTypes[dimension] = timeDimensions[dimension] ? ColumnType.LONG : dimensionTypes[dimension];
            keys[dimension] = ResultFrameBuilder.newColumn(keyStorageTypes[dimension], groupCapacity);
        }
        measurePresent = new boolean[measureColumns.length][groupCapacity];
        measures = new Object[measureColumns.length];
        for (int measure = 0; measure < measureColumns.length; measure++) {
            measures[measure] = ResultFrameBuilder.newColumn(measureTypes[measure], groupCapacity);
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
                    case DOUBLE -> Double.hashCode(SparkValues.canonicalDouble(input[column].doubleValue(row)));
                    case STRING -> input[column].stringValue(row).hashCode();
                    case DECIMAL -> SparkValues.canonicalDecimal(input[column].decimalValue(row)).hashCode();
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
            if (timeDimensions[dimension]) ((long[]) keys[dimension])[group] = timeValue(input, row, dimension);
            else switch (dimensionTypes[dimension]) {
                case LONG -> ((long[]) keys[dimension])[group] = input[column].longValue(row);
                case DOUBLE -> ((double[]) keys[dimension])[group] = SparkValues.canonicalDouble(input[column].doubleValue(row));
                case STRING -> ((String[]) keys[dimension])[group] = input[column].stringValue(row);
                case DECIMAL -> ((BigDecimal[]) keys[dimension])[group] = input[column].decimalValue(row);
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
                if (timeValue(input, row, dimension) != ((long[]) keys[dimension])[group]) return false;
                continue;
            }
            boolean equal = switch (dimensionTypes[dimension]) {
                case LONG -> input[column].longValue(row) == ((long[]) keys[dimension])[group];
                case DOUBLE -> Double.doubleToLongBits(SparkValues.canonicalDouble(input[column].doubleValue(row)))
                        == Double.doubleToLongBits(((double[]) keys[dimension])[group]);
                case STRING -> input[column].stringValue(row).equals(((String[]) keys[dimension])[group]);
                case DECIMAL -> input[column].decimalValue(row).compareTo(((BigDecimal[]) keys[dimension])[group]) == 0;
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
            case STRING -> ((Number) (Object) frame[column].stringValue(row)).longValue();
        };
        return com.cascada.cache.domain.time.BucketMath.floor(timestamp, step);
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
        long[] values = (long[]) measures[measure];
        if (!measurePresent[measure][group]) {
            values[group] = incoming;
            measurePresent[measure][group] = true;
            return;
        }
        long previous = values[group];
        values[group] = switch (measureFunctions[measure]) {
            case SUM, COUNT -> Math.addExact(previous, incoming);
            case MINIMUM -> Math.min(previous, incoming);
            case MAXIMUM -> Math.max(previous, incoming);
        };
    }

    void accumulateDouble(int group, int measure, double incoming) {
        double[] values = (double[]) measures[measure];
        if (!measurePresent[measure][group]) {
            values[group] = incoming;
            measurePresent[measure][group] = true;
            return;
        }
        values[group] = measureFunctions[measure].combine(values[group], incoming);
    }

    void accumulateString(int group, int measure, String incoming) {
        String[] values = (String[]) measures[measure];
        if (!measurePresent[measure][group]) {
            values[group] = incoming;
            measurePresent[measure][group] = true;
            return;
        }
        String previous = values[group];
        values[group] = switch (measureFunctions[measure]) {
            case MINIMUM -> SparkValues.compareStrings(previous, incoming) <= 0 ? previous : incoming;
            case MAXIMUM -> SparkValues.compareStrings(previous, incoming) >= 0 ? previous : incoming;
            case SUM, COUNT -> throw new IllegalArgumentException(
                    "STRING aggregate column requires MIN or MAX, got " + measureFunctions[measure]);
        };
    }

    void accumulateDecimal(int group, int measure, BigDecimal incoming) {
        BigDecimal[] values = (BigDecimal[]) measures[measure];
        String column = schema.columnNames().get(measureColumns[measure]);
        SparkValues.requireDecimal(incoming, column);
        if (!measurePresent[measure][group]) {
            values[group] = incoming;
            measurePresent[measure][group] = true;
            return;
        }
        BigDecimal previous = values[group];
        values[group] = switch (measureFunctions[measure]) {
            case SUM, COUNT -> SparkValues.decimalSum(previous, incoming, column);
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
            if (keys[dimension] != null) {
                keys[dimension] = ResultFrameBuilder.growColumn(keys[dimension], keyStorageTypes[dimension], grown);
            }
        }
        for (int measure = 0; measure < measureColumns.length; measure++) {
            measurePresent[measure] = Arrays.copyOf(measurePresent[measure], grown);
            measures[measure] = ResultFrameBuilder.growColumn(measures[measure], measureTypes[measure], grown);
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

    int[] sortedGroupIds(List<String> dimensions) {
        int[] order = new int[groupCount];
        for (int group = 0; group < groupCount; group++) {
            order[group] = group;
        }
        if (!dimensions.isEmpty()) {
            IntStableSort.sort(order, (left, right) -> compareGroups(left, right, dimensions.size()));
        }
        return order;
    }

    int compareGroups(int left, int right, int dimensionCount) {
        for (int dimension = 0; dimension < dimensionCount; dimension++) {
            boolean a = keyPresent[dimension][left], b = keyPresent[dimension][right];
            int comparison;
            if (!a || !b) comparison = a == b ? 0 : a ? 1 : -1;
            else comparison = switch (keyStorageTypes[dimension]) {
                case LONG -> Long.compare(((long[]) keys[dimension])[left], ((long[]) keys[dimension])[right]);
                case DOUBLE -> Double.compare(((double[]) keys[dimension])[left], ((double[]) keys[dimension])[right]);
                case STRING -> SparkValues.compareStrings(((String[]) keys[dimension])[left], ((String[]) keys[dimension])[right]);
                case DECIMAL -> ((BigDecimal[]) keys[dimension])[left].compareTo(((BigDecimal[]) keys[dimension])[right]);
            };
            if (comparison != 0) return comparison;
        }
        return 0;
    }

    void appendDimension(ResultFrameBuilder output, int group, int dimension,
                                 ColumnType outputType, boolean time) {
        if (!keyPresent[dimension][group]) { output.appendNull(); return; }
        if (time) {
            long value = ((long[]) keys[dimension])[group];
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
        appendStored(output, keys[dimension], outputType, group);
    }

    void appendMeasure(ResultFrameBuilder output, int group, int measure) {
        if (measure < 0 || !measurePresent[measure][group]) {
            output.appendNull();
            return;
        }
        appendStored(output, measures[measure], measureTypes[measure], group);
    }

    private static void appendStored(ResultFrameBuilder output, Object column, ColumnType type, int group) {
        switch (type) {
            case LONG -> output.appendLong(((long[]) column)[group]);
            case DOUBLE -> output.appendDouble(((double[]) column)[group]);
            case STRING -> output.appendString(((String[]) column)[group]);
            case DECIMAL -> output.appendDecimal(((BigDecimal[]) column)[group]);
        }
    }
}
