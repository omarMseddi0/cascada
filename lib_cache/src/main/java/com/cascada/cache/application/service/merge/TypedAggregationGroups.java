package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.time.BucketMath;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Open-addressing group table with row-major, paged group state.
 *
 * <p>Every group owns one contiguous run of {@code long} words (primitive keys, primitive measures,
 * dictionary ids of STRING measures, null flags and the group hash) plus, only when the schema needs
 * them, object slots for STRING/DECIMAL keys and DECIMAL measures. Merging one input row therefore
 * touches one or two cache lines instead of one line per key/measure array. State is stored in
 * fixed-size pages, so adding groups never copies existing state or over-allocates it by doubling.
 *
 * <p>A slot stores {@code hashTag << 32 | groupId + 1}; a probe rejects a different group by its tag
 * without reading that group's state. A frame is merged column-at-a-time: one hashing pass, one
 * probing pass, then one tight loop per measure column with the type resolved once per column.
 */
final class TypedAggregationGroups {
    private static final int GROUPS_PER_PAGE_SHIFT = 14;
    private static final int GROUPS_PER_PAGE = 1 << GROUPS_PER_PAGE_SHIFT;
    private static final int GROUP_IN_PAGE_MASK = GROUPS_PER_PAGE - 1;
    private static final int INITIAL_PAGE_TABLE_LENGTH = 4;
    private static final int INITIAL_SLOT_COUNT = 1024;
    private static final long EMPTY_SLOT = 0L;
    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;
    private static final long HASH_MULTIPLIER = 0xFF51AFD7ED558CCDL;
    private static final long HASH_FINAL_MULTIPLIER = 0xC4CEB9FE1A85EC53L;
    private static final long NULL_KEY_HASH = 0x5BD1E995L;
    private static final int NOT_STORED = -1;
    private static final int NOT_OWNED = -1;
    private static final int PARTITION_HASH_SHIFT = 40;
    private static final int PARTITION_FRACTION_BITS = Long.SIZE - PARTITION_HASH_SHIFT;

    private final ResultFrame schema;
    private final int[] dimensionColumns;
    private final ColumnType[] dimensionTypes;
    private final ColumnType[] keyStorageTypes;
    private final boolean[] timeDimensions;
    private final int step;
    private final int[] measureColumns;
    private final ColumnType[] measureTypes;
    private final AggregateOperation[] measureFunctions;
    private final int partitionIndex;
    private final int partitionCount;

    private final int[] dimensionWordOffsets;
    private final int[] dimensionObjectOffsets;
    private final int[] measureWordOffsets;
    private final int[] measureObjectOffsets;
    private final int flagWordOffset;
    private final int hashWordOffset;
    private final int wordsPerGroup;
    private final int objectsPerGroup;

    private final List<List<String>> stringMeasureDictionaries = new ArrayList<>();
    private final List<Map<String, Integer>> stringMeasureDictionaryIds = new ArrayList<>();
    private final String[] lastDictionaryValues;
    private final int[] lastDictionaryIds;

    private long[][] wordPages = new long[INITIAL_PAGE_TABLE_LENGTH][];
    private Object[][] objectPages = new Object[INITIAL_PAGE_TABLE_LENGTH][];
    private long[] slots = new long[INITIAL_SLOT_COUNT];
    private int slotMask = INITIAL_SLOT_COUNT - 1;
    private long[] rowHashes = new long[0];
    private int[] rowGroups = new int[0];
    /** Reused by this table; same-partition frame work is chained, so only one task uses it at a time. */
    private int[] partitionGroupScratch = new int[0];
    int groupCount;


    TypedAggregationGroups(ResultFrame schema, int[] dimensionColumns, ColumnType[] dimensionTypes,
                           boolean[] timeDimensions, int step, int[] measureColumns,
                           ColumnType[] measureTypes, AggregateOperation[] measureFunctions,
                           int partitionIndex, int partitionCount) {
        this.schema = schema;
        this.dimensionColumns = dimensionColumns;
        this.dimensionTypes = dimensionTypes;
        this.timeDimensions = timeDimensions;
        this.step = step;
        this.measureColumns = measureColumns;
        this.measureTypes = measureTypes;
        this.measureFunctions = measureFunctions;
        this.partitionIndex = partitionIndex;
        this.partitionCount = partitionCount;

        int dimensionCount = dimensionColumns.length;
        int measureCount = measureColumns.length;
        keyStorageTypes = new ColumnType[dimensionCount];
        dimensionWordOffsets = new int[dimensionCount];
        dimensionObjectOffsets = new int[dimensionCount];
        measureWordOffsets = new int[measureCount];
        measureObjectOffsets = new int[measureCount];
        Arrays.fill(dimensionWordOffsets, NOT_STORED);
        Arrays.fill(dimensionObjectOffsets, NOT_STORED);
        Arrays.fill(measureWordOffsets, NOT_STORED);
        Arrays.fill(measureObjectOffsets, NOT_STORED);

        int nextWord = 0;
        int nextObject = 0;
        for (int dimension = 0; dimension < dimensionCount; dimension++) {
            if (dimensionColumns[dimension] < 0) continue;
            // A time dimension is floored to an epoch-second bucket start and kept as a LONG.
            keyStorageTypes[dimension] = timeDimensions[dimension] ? ColumnType.LONG : dimensionTypes[dimension];
            if (isObjectStored(keyStorageTypes[dimension])) dimensionObjectOffsets[dimension] = nextObject++;
            else dimensionWordOffsets[dimension] = nextWord++;
        }
        for (int measure = 0; measure < measureCount; measure++) {
            if (measureTypes[measure] == ColumnType.DECIMAL) measureObjectOffsets[measure] = nextObject++;
            else measureWordOffsets[measure] = nextWord++;
            stringMeasureDictionaries.add(new ArrayList<>());
            stringMeasureDictionaryIds.add(new HashMap<>());
        }
        flagWordOffset = nextWord;
        nextWord += (dimensionCount + measureCount + Long.SIZE - 1) / Long.SIZE;
        hashWordOffset = nextWord++;
        wordsPerGroup = nextWord;
        objectsPerGroup = nextObject;
        lastDictionaryValues = new String[measureCount];
        lastDictionaryIds = new int[measureCount];
    }

    private static boolean isObjectStored(ColumnType type) {
        return type == ColumnType.STRING || type == ColumnType.DECIMAL;
    }

    void accumulateFrame(FrameColumnReader[] input, int rowCount) {
        if (rowHashes.length < rowCount) {
            rowHashes = new long[rowCount];
            rowGroups = new int[rowCount];
        }
        computeRowHashes(input, rowCount, rowHashes);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            rowGroups[row] = ownsHash(rowHashes[row]) ? findOrCreate(input, row, rowHashes[row]) : NOT_OWNED;
        }
        for (int measure = 0; measure < measureColumns.length; measure++) {
            FrameColumnReader column = input[measureColumns[measure]];
            accumulateColumn(measure, column, rowCount, null, null, 0);
        }
    }

    /** Computes hashes once, then groups row indices by partition without changing their order. */
    PartitionedFrame partitionFrame(FrameColumnReader[] input, int rowCount, PartitionedFrame reusable) {
        PartitionedFrame frame = reusable == null ? new PartitionedFrame(partitionCount) : reusable;
        frame.columns = input;
        if (frame.hashes.length < rowCount) {
            frame.hashes = new long[rowCount];
            frame.rows = new int[rowCount];
        }
        computeRowHashes(input, rowCount, frame.hashes);
        Arrays.fill(frame.offsets, 0);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            frame.offsets[partitionForHash(frame.hashes[row], partitionCount) + 1]++;
        }
        for (int partition = 0; partition < partitionCount; partition++) {
            frame.offsets[partition + 1] += frame.offsets[partition];
        }
        int[] cursors = Arrays.copyOf(frame.offsets, partitionCount);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            int partition = partitionForHash(frame.hashes[row], partitionCount);
            frame.rows[cursors[partition]++] = row;
        }
        return frame;
    }

    void accumulateFrame(PartitionedFrame frame) {
        int start = frame.offsets[partitionIndex];
        int count = frame.offsets[partitionIndex + 1] - start;
        if (partitionGroupScratch.length < count) {
            partitionGroupScratch = new int[count];
        }
        int[] groups = partitionGroupScratch;
        for (int position = 0; position < count; position++) {
            if ((position & 1023) == 0) QueryCancellation.requireCurrentActive();
            int row = frame.rows[start + position];
            groups[position] = findOrCreate(frame.columns, row, frame.hashes[row]);
        }
        for (int measure = 0; measure < measureColumns.length; measure++) {
            accumulateColumn(measure, frame.columns[measureColumns[measure]], count, frame.rows, groups, start);
        }
    }

    private void computeRowHashes(FrameColumnReader[] input, int rowCount, long[] hashes) {
        Arrays.fill(hashes, 0, rowCount, HASH_SEED);
        for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
            QueryCancellation.requireCurrentActive();
            int column = dimensionColumns[dimension];
            if (column < 0) {
                for (int row = 0; row < rowCount; row++) {
                    if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                    hashes[row] = mixHash(hashes[row], NULL_KEY_HASH);
                }
                continue;
            }
            FrameColumnReader reader = input[column];
            for (int row = 0; row < rowCount; row++) {
                if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                long keyHash = reader.isNullAt(row) ? NULL_KEY_HASH : keyHash(reader, row, dimension);
                hashes[row] = mixHash(hashes[row], keyHash);
            }
        }
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            hashes[row] = finalizeHash(hashes[row]);
        }
    }

    /** Partitions split the hash space, so every group and its summation order have exactly one owner. */
    private boolean ownsHash(long hash) {
        return partitionCount == 1
                || partitionForHash(hash, partitionCount) == partitionIndex;
    }

    private static int partitionForHash(long hash, int partitionCount) {
        return (int) ((hash >>> PARTITION_HASH_SHIFT) * partitionCount >>> PARTITION_FRACTION_BITS);
    }

    private long keyHash(FrameColumnReader reader, int row, int dimension) {
        if (isWordStored(dimension)) return storedDimensionWord(reader, row, dimension);
        return dimensionTypes[dimension] == ColumnType.STRING
                ? reader.stringValue(row).hashCode()
                : SparkValues.canonicalDecimal(reader.decimalValue(row)).hashCode();
    }

    private boolean isWordStored(int dimension) {
        return dimensionWordOffsets[dimension] != NOT_STORED;
    }

    /** A primitive key as stored and compared: the floored bucket, the LONG value, or the canonical DOUBLE's bits. */
    private long storedDimensionWord(FrameColumnReader reader, int row, int dimension) {
        if (timeDimensions[dimension]) return timeValue(reader, row, dimension);
        return dimensionTypes[dimension] == ColumnType.LONG
                ? reader.longValue(row)
                : Double.doubleToRawLongBits(SparkValues.canonicalDouble(reader.doubleValue(row)));
    }

    private static long mixHash(long hash, long keyHash) {
        return (hash ^ keyHash) * HASH_MULTIPLIER;
    }

    private static long finalizeHash(long hash) {
        long mixed = (hash ^ (hash >>> 33)) * HASH_FINAL_MULTIPLIER;
        return mixed ^ (mixed >>> 33);
    }

    private int findOrCreate(FrameColumnReader[] input, int row, long hash) {
        long hashTag = hash >>> Integer.SIZE;
        int slot = (int) hash & slotMask;
        for (long entry = slots[slot]; entry != EMPTY_SLOT; entry = slots[slot]) {
            int group = (int) entry - 1;
            if (entry >>> Integer.SIZE == hashTag && sameDimensions(group, input, row)) return group;
            slot = (slot + 1) & slotMask;
        }
        if ((groupCount + 1L) * 2 > slots.length) {
            growSlots();
            slot = (int) hash & slotMask;
            while (slots[slot] != EMPTY_SLOT) slot = (slot + 1) & slotMask;
        }
        int group = groupCount++;
        ensurePage(group >>> GROUPS_PER_PAGE_SHIFT);
        wordPage(group)[wordBase(group) + hashWordOffset] = hash;
        copyDimensions(group, input, row);
        slots[slot] = hashTag << Integer.SIZE | (group + 1L);
        return group;
    }

    private void ensurePage(int page) {
        if (page == wordPages.length) {
            wordPages = Arrays.copyOf(wordPages, page * 2);
            objectPages = Arrays.copyOf(objectPages, page * 2);
        }
        if (wordPages[page] != null) return;
        wordPages[page] = new long[GROUPS_PER_PAGE * wordsPerGroup];
        if (objectsPerGroup > 0) objectPages[page] = new Object[GROUPS_PER_PAGE * objectsPerGroup];
    }

    private void copyDimensions(int group, FrameColumnReader[] input, int row) {
        long[] words = wordPage(group);
        int wordBase = wordBase(group);
        for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
            int column = dimensionColumns[dimension];
            if (column < 0 || input[column].isNullAt(row)) continue;
            setFlag(words, wordBase, dimension);
            FrameColumnReader reader = input[column];
            if (isWordStored(dimension)) {
                words[wordBase + dimensionWordOffsets[dimension]] = storedDimensionWord(reader, row, dimension);
            } else {
                objectPage(group)[objectBase(group) + dimensionObjectOffsets[dimension]] =
                        dimensionTypes[dimension] == ColumnType.STRING ? reader.stringValue(row) : reader.decimalValue(row);
            }
        }
    }

    private boolean sameDimensions(int group, FrameColumnReader[] input, int row) {
        long[] words = wordPage(group);
        int wordBase = wordBase(group);
        for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
            int column = dimensionColumns[dimension];
            if (column < 0) continue;
            FrameColumnReader reader = input[column];
            boolean present = !reader.isNullAt(row);
            if (present != hasFlag(words, wordBase, dimension)) return false;
            if (present && !sameDimensionValue(group, words, wordBase, reader, row, dimension)) return false;
        }
        return true;
    }

    private boolean sameDimensionValue(int group, long[] words, int wordBase, FrameColumnReader reader,
                                       int row, int dimension) {
        if (isWordStored(dimension)) {
            return storedDimensionWord(reader, row, dimension) == words[wordBase + dimensionWordOffsets[dimension]];
        }
        Object stored = dimensionObject(group, dimension);
        return dimensionTypes[dimension] == ColumnType.STRING
                ? reader.stringValue(row).equals(stored)
                : reader.decimalValue(row).compareTo((BigDecimal) stored) == 0;
    }

    private long timeValue(FrameColumnReader reader, int row, int dimension) {
        long timestamp = switch (dimensionTypes[dimension]) {
            case LONG -> reader.longValue(row);
            case DOUBLE -> (long) reader.doubleValue(row);
            case DECIMAL -> reader.decimalValue(row).longValue();
            case STRING -> ((Number) (Object) reader.stringValue(row)).longValue();
        };
        return BucketMath.floor(timestamp, step);
    }

    private void accumulateColumn(int measure, FrameColumnReader column, int rowCount,
                                  int[] rows, int[] groups, int rowOffset) {
        switch (measureTypes[measure]) {
            case LONG -> accumulateLongColumn(measure, column, rowCount, rows, groups, rowOffset);
            case DOUBLE -> accumulateDoubleColumn(measure, column, rowCount, rows, groups, rowOffset);
            case STRING -> accumulateStringColumn(measure, column, rowCount, rows, groups, rowOffset);
            case DECIMAL -> accumulateDecimalColumn(measure, column, rowCount, rows, groups, rowOffset);
        }
    }

    private void accumulateLongColumn(int measure, FrameColumnReader column, int rowCount,
                                      int[] rows, int[] groups, int rowOffset) {
        int valueOffset = measureWordOffsets[measure];
        int flag = measureFlag(measure);
        AggregateOperation function = measureFunctions[measure];
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            int inputRow = rows == null ? row : rows[rowOffset + row];
            int group = rows == null ? rowGroups[row] : groups[row];
            if (group == NOT_OWNED || column.isNullAt(inputRow)) continue;
            long[] words = wordPage(group);
            int wordBase = wordBase(group);
            long incoming = column.longValue(inputRow);
            if (!hasFlag(words, wordBase, flag)) {
                words[wordBase + valueOffset] = incoming;
                setFlag(words, wordBase, flag);
                continue;
            }
            long previous = words[wordBase + valueOffset];
            words[wordBase + valueOffset] = switch (function) {
                case SUM, COUNT -> Math.addExact(previous, incoming);
                case MINIMUM -> Math.min(previous, incoming);
                case MAXIMUM -> Math.max(previous, incoming);
            };
        }
    }

    private void accumulateDoubleColumn(int measure, FrameColumnReader column, int rowCount,
                                        int[] rows, int[] groups, int rowOffset) {
        int valueOffset = measureWordOffsets[measure];
        int flag = measureFlag(measure);
        AggregateOperation function = measureFunctions[measure];
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            int inputRow = rows == null ? row : rows[rowOffset + row];
            int group = rows == null ? rowGroups[row] : groups[row];
            if (group == NOT_OWNED || column.isNullAt(inputRow)) continue;
            long[] words = wordPage(group);
            int wordBase = wordBase(group);
            double incoming = column.doubleValue(inputRow);
            if (!hasFlag(words, wordBase, flag)) {
                words[wordBase + valueOffset] = Double.doubleToRawLongBits(incoming);
                setFlag(words, wordBase, flag);
                continue;
            }
            double previous = Double.longBitsToDouble(words[wordBase + valueOffset]);
            words[wordBase + valueOffset] = Double.doubleToRawLongBits(function.combine(previous, incoming));
        }
    }

    private void accumulateStringColumn(int measure, FrameColumnReader column, int rowCount,
                                        int[] rows, int[] groups, int rowOffset) {
        int valueOffset = measureWordOffsets[measure];
        int flag = measureFlag(measure);
        AggregateOperation function = measureFunctions[measure];
        List<String> dictionary = stringMeasureDictionaries.get(measure);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            int inputRow = rows == null ? row : rows[rowOffset + row];
            int group = rows == null ? rowGroups[row] : groups[row];
            if (group == NOT_OWNED || column.isNullAt(inputRow)) continue;
            long[] words = wordPage(group);
            int wordBase = wordBase(group);
            int incomingId = dictionaryId(measure, column.stringValue(inputRow));
            if (!hasFlag(words, wordBase, flag)) {
                words[wordBase + valueOffset] = incomingId;
                setFlag(words, wordBase, flag);
                continue;
            }
            int previousId = (int) words[wordBase + valueOffset];
            words[wordBase + valueOffset] = switch (function) {
                case MINIMUM -> previousId == incomingId
                        || SparkValues.compareStrings(dictionary.get(previousId), dictionary.get(incomingId)) <= 0
                        ? previousId : incomingId;
                case MAXIMUM -> previousId == incomingId
                        || SparkValues.compareStrings(dictionary.get(previousId), dictionary.get(incomingId)) >= 0
                        ? previousId : incomingId;
                case SUM, COUNT -> throw new IllegalArgumentException(
                        "STRING aggregate column requires MIN or MAX, got " + function);
            };
        }
    }

    /** Equal values share one id, so MIN/MAX compare strings only when two different values meet. */
    private int dictionaryId(int measure, String value) {
        if (value == lastDictionaryValues[measure]) return lastDictionaryIds[measure];
        List<String> dictionary = stringMeasureDictionaries.get(measure);
        int id = stringMeasureDictionaryIds.get(measure).computeIfAbsent(value, newValue -> {
            dictionary.add(newValue);
            return dictionary.size() - 1;
        });
        lastDictionaryValues[measure] = value;
        lastDictionaryIds[measure] = id;
        return id;
    }

    private void accumulateDecimalColumn(int measure, FrameColumnReader column, int rowCount,
                                         int[] rows, int[] groups, int rowOffset) {
        int valueOffset = measureObjectOffsets[measure];
        int flag = measureFlag(measure);
        AggregateOperation function = measureFunctions[measure];
        String columnName = schema.columnNames().get(measureColumns[measure]);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            int inputRow = rows == null ? row : rows[rowOffset + row];
            int group = rows == null ? rowGroups[row] : groups[row];
            if (group == NOT_OWNED || column.isNullAt(inputRow)) continue;
            long[] words = wordPage(group);
            int wordBase = wordBase(group);
            Object[] objects = objectPage(group);
            int objectSlot = objectBase(group) + valueOffset;
            BigDecimal incoming = column.decimalValue(inputRow);
            SparkValues.requireDecimal(incoming, columnName);
            if (!hasFlag(words, wordBase, flag)) {
                objects[objectSlot] = incoming;
                setFlag(words, wordBase, flag);
                continue;
            }
            BigDecimal previous = (BigDecimal) objects[objectSlot];
            objects[objectSlot] = switch (function) {
                case SUM, COUNT -> SparkValues.decimalSum(previous, incoming, columnName);
                case MINIMUM -> previous.min(incoming);
                case MAXIMUM -> previous.max(incoming);
            };
        }
    }

    private void growSlots() {
        if (slots.length > Integer.MAX_VALUE / 2) {
            throw new IllegalStateException("too many distinct groups to merge in one frame");
        }
        long[] grownSlots = new long[slots.length * 2];
        int grownMask = grownSlots.length - 1;
        for (long entry : slots) {
            if (entry == EMPTY_SLOT) continue;
            int group = (int) entry - 1;
            int slot = (int) wordPage(group)[wordBase(group) + hashWordOffset] & grownMask;
            while (grownSlots[slot] != EMPTY_SLOT) slot = (slot + 1) & grownMask;
            grownSlots[slot] = entry;
        }
        slots = grownSlots;
        slotMask = grownMask;
    }

    int[] sortedGroupIds(List<String> dimensions) {
        int[] order = new int[groupCount];
        Arrays.setAll(order, group -> group);
        if (!dimensions.isEmpty()) {
            IntStableSort.sort(order, (left, right) -> compareGroups(left, right, dimensions.size()));
        }
        return order;
    }

    int compareGroups(int left, int right, int dimensionCount) {
        return compareWithGroupOf(left, this, right, dimensionCount);
    }

    /** Orders a group of this partition against a group of {@code otherPartition}; both share one schema. */
    int compareWithGroupOf(int group, TypedAggregationGroups otherPartition, int otherGroup, int dimensionCount) {
        for (int dimension = 0; dimension < dimensionCount; dimension++) {
            int comparison = compareDimension(group, otherPartition, otherGroup, dimension);
            if (comparison != 0) return comparison;
        }
        return 0;
    }

    private int compareDimension(int group, TypedAggregationGroups otherPartition, int otherGroup, int dimension) {
        boolean present = isDimensionPresent(group, dimension);
        boolean otherPresent = otherPartition.isDimensionPresent(otherGroup, dimension);
        if (!present || !otherPresent) return present == otherPresent ? 0 : present ? 1 : -1;
        return switch (keyStorageTypes[dimension]) {
            case LONG -> Long.compare(dimensionWord(group, dimension), otherPartition.dimensionWord(otherGroup, dimension));
            case DOUBLE -> Double.compare(Double.longBitsToDouble(dimensionWord(group, dimension)),
                    Double.longBitsToDouble(otherPartition.dimensionWord(otherGroup, dimension)));
            case STRING -> SparkValues.compareStrings((String) dimensionObject(group, dimension),
                    (String) otherPartition.dimensionObject(otherGroup, dimension));
            case DECIMAL -> ((BigDecimal) dimensionObject(group, dimension))
                    .compareTo((BigDecimal) otherPartition.dimensionObject(otherGroup, dimension));
        };
    }

    void appendDimension(ResultFrameBuilder output, int group, int dimension, ColumnType outputType, boolean time) {
        if (!isDimensionPresent(group, dimension)) {
            output.appendNull();
            return;
        }
        if (time) {
            long value = dimensionWord(group, dimension);
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
        switch (keyStorageTypes[dimension]) {
            case LONG -> output.appendLong(dimensionWord(group, dimension));
            case DOUBLE -> output.appendDouble(Double.longBitsToDouble(dimensionWord(group, dimension)));
            case STRING -> output.appendString((String) dimensionObject(group, dimension));
            case DECIMAL -> output.appendDecimal((BigDecimal) dimensionObject(group, dimension));
        }
    }

    void appendMeasure(ResultFrameBuilder output, int group, int measure) {
        if (measure < 0 || !hasMeasure(group, measure)) {
            output.appendNull();
            return;
        }
        switch (measureTypes[measure]) {
            case LONG -> output.appendLong(measureWord(group, measure));
            case DOUBLE -> output.appendDouble(Double.longBitsToDouble(measureWord(group, measure)));
            case STRING -> output.appendString(stringMeasure(group, measure));
            case DECIMAL -> output.appendDecimal(decimalMeasure(group, measure));
        }
    }

    void appendAverage(ResultFrameBuilder output, int group, int sumMeasure, int countMeasure,
                       ColumnType resultType, String inputType) {
        long count = hasMeasure(group, countMeasure) ? SparkAverage.exactCount(numericMeasure(group, countMeasure)) : 0L;
        Number sum = hasMeasure(group, sumMeasure) ? numericMeasure(group, sumMeasure) : null;
        SparkAverage.appendAverage(output, sum, count, resultType, inputType);
    }

    /** The group's merged STRING measure, or {@code null} when every input value was NULL. */
    String stringMeasure(int group, int measure) {
        return hasMeasure(group, measure)
                ? stringMeasureDictionaries.get(measure).get((int) measureWord(group, measure))
                : null;
    }

    private boolean hasMeasure(int group, int measure) {
        return hasFlag(wordPage(group), wordBase(group), measureFlag(measure));
    }

    private Number numericMeasure(int group, int measure) {
        return switch (measureTypes[measure]) {
            case LONG -> measureWord(group, measure);
            case DOUBLE -> Double.longBitsToDouble(measureWord(group, measure));
            case DECIMAL -> decimalMeasure(group, measure);
            case STRING -> throw new IllegalArgumentException("AVG ingredient '"
                    + schema.columnNames().get(measureColumns[measure]) + "' must be numeric");
        };
    }

    private BigDecimal decimalMeasure(int group, int measure) {
        return (BigDecimal) objectPage(group)[objectBase(group) + measureObjectOffsets[measure]];
    }

    private boolean isDimensionPresent(int group, int dimension) {
        return dimensionColumns[dimension] >= 0 && hasFlag(wordPage(group), wordBase(group), dimension);
    }

    private long dimensionWord(int group, int dimension) {
        return wordPage(group)[wordBase(group) + dimensionWordOffsets[dimension]];
    }

    private Object dimensionObject(int group, int dimension) {
        return objectPage(group)[objectBase(group) + dimensionObjectOffsets[dimension]];
    }

    private long measureWord(int group, int measure) {
        return wordPage(group)[wordBase(group) + measureWordOffsets[measure]];
    }

    private int measureFlag(int measure) {
        return dimensionColumns.length + measure;
    }

    private boolean hasFlag(long[] words, int wordBase, int flag) {
        return (words[wordBase + flagWordOffset + (flag >>> 6)] & (1L << flag)) != 0;
    }

    private void setFlag(long[] words, int wordBase, int flag) {
        words[wordBase + flagWordOffset + (flag >>> 6)] |= 1L << flag;
    }

    private long[] wordPage(int group) {
        return wordPages[group >>> GROUPS_PER_PAGE_SHIFT];
    }

    private Object[] objectPage(int group) {
        return objectPages[group >>> GROUPS_PER_PAGE_SHIFT];
    }

    private int wordBase(int group) {
        return (group & GROUP_IN_PAGE_MASK) * wordsPerGroup;
    }

    private int objectBase(int group) {
        return (group & GROUP_IN_PAGE_MASK) * objectsPerGroup;
    }
}
