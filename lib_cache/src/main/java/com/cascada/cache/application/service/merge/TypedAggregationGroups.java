package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.time.BucketMath;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Open-addressing group table with row-major, paged group state.
 *
 * <p>Every group owns one contiguous run of {@code long} words (primitive keys, primitive measures,
 * null flags and rehash bits) plus, only when the schema needs
 * them, object slots for STRING/DECIMAL keys and DECIMAL measures. STRING aggregate winners use
 * constant storage with sparse exception pages. The fused row update keeps each group state hot.
 * Pages hold 1,024 groups. The first page grows from 16 groups; later pages never copy all existing
 * group payload or double its entire capacity.
 *
 * <p>A slot stores {@code hashTag << 32 | groupId + 1}; a probe rejects a different group by its tag
 * without reading that group's state. A frame is hashed once. Each owned row probes its group and updates all measures
 * before moving on, preserving locality across the row-major state.
 */
final class TypedAggregationGroups {
    private static final int GROUPS_PER_PAGE_SHIFT = 10;
    private static final int GROUPS_PER_PAGE = 1 << GROUPS_PER_PAGE_SHIFT;
    private static final int GROUP_IN_PAGE_MASK = GROUPS_PER_PAGE - 1;
    private static final int INITIAL_PAGE_TABLE_LENGTH = 4;
    private static final int INITIAL_SLOT_COUNT = 16;
    private static final long EMPTY_SLOT = 0L;
    private static final long HASH_SEED = 0x9E3779B97F4A7C15L;
    private static final long HASH_MULTIPLIER = 0xFF51AFD7ED558CCDL;
    private static final long HASH_FINAL_MULTIPLIER = 0xC4CEB9FE1A85EC53L;
    private static final long NULL_KEY_HASH = 0x5BD1E995L;
    private static final int NOT_STORED = -1;
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
    private final boolean packedHash;
    private final int wordsPerGroup;
    private final int objectsPerGroup;

    private final GroupedStringValues[] stringMeasures;

    private long[][] wordPages = new long[INITIAL_PAGE_TABLE_LENGTH][];
    private Object[][] objectPages = new Object[INITIAL_PAGE_TABLE_LENGTH][];
    private long[] slots = new long[INITIAL_SLOT_COUNT];
    private int slotMask = INITIAL_SLOT_COUNT - 1;
    private long[] rowHashes = new long[0];
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
        stringMeasures = new GroupedStringValues[measureCount];
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
            else if (measureTypes[measure] == ColumnType.STRING) {
                stringMeasures[measure] = new GroupedStringValues(measureFunctions[measure], GROUPS_PER_PAGE_SHIFT);
            } else measureWordOffsets[measure] = nextWord++;
        }
        flagWordOffset = nextWord;
        int flagCount = dimensionCount + measureCount;
        int flagWords = (flagCount + Long.SIZE - 1) / Long.SIZE;
        nextWord += flagWords;
        // Rehashing only needs the low 32 hash bits; tags remain in the slot entries. Use the
        // spare upper half of the final validity word when no flag can occupy those bits.
        packedHash = flagWords > 0 && ((flagCount - 1) & 63) < 32;
        hashWordOffset = packedHash ? flagWordOffset + flagWords - 1 : nextWord++;
        wordsPerGroup = nextWord;
        objectsPerGroup = nextObject;
    }

    private static boolean isObjectStored(ColumnType type) {
        return type == ColumnType.STRING || type == ColumnType.DECIMAL;
    }

    void accumulateFrame(FrameColumnReader[] input, int rowCount) {
        if (rowHashes.length < rowCount) {
            rowHashes = new long[rowCount];
        }
        computeRowHashes(input, rowCount, rowHashes);
        for (int row = 0; row < rowCount; row++) {
            if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
            if (ownsHash(rowHashes[row])) {
                int group = findOrCreate(input, row, rowHashes[row]);
                accumulateRow(input, row, group);
            }
        }
    }

    /** Computes hashes once, then groups row indices by partition without changing their order. */
    CompletableFuture<Void> partitionFrame(FrameColumnReader[] input, int rowCount, PartitionedFrame frame, Executor executor) {
        frame.columns = input;
        if (frame.hashes.length < rowCount) {
            frame.hashes = new long[rowCount];
            frame.rows = new int[rowCount];
        }
        boolean manyWorkers = partitionCount > 16 && rowCount >= Math.max(32_768L, partitionCount * 512L);
        if ((rowCount >= 262_144 || manyWorkers) && partitionCount > 1) {
            return partitionInParallel(frame, rowCount, executor);
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
        return CompletableFuture.completedFuture(null);
    }

    /** Stable parallel count/prefix/scatter: each chunk owns disjoint input and output ranges. */
    private CompletableFuture<Void> partitionInParallel(PartitionedFrame frame, int rowCount, Executor executor) {
        int chunks = partitionCount > 16 ? Math.min(partitionCount, Math.max(1, rowCount / 512))
                : (int) Math.min(partitionCount, ((long) rowCount + 65_535) / 65_536);
        int[][] cursors = new int[chunks][partitionCount];
        CompletableFuture<?>[] work = new CompletableFuture<?>[chunks];
        for (int chunk = 0; chunk < chunks; chunk++) {
            int start = (int) ((long) rowCount * chunk / chunks);
            int end = (int) ((long) rowCount * (chunk + 1) / chunks);
            int[] counts = cursors[chunk];
            work[chunk] = CompletableFuture.runAsync(QueryExecutionScope.propagate(() -> {
                computeRowHashes(frame.columns, start, end, frame.hashes);
                for (int row = start; row < end; row++) {
                    if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                    counts[partitionForHash(frame.hashes[row], partitionCount)]++;
                }
            }), executor);
        }
        // No executor worker blocks awaiting its children. The last count task schedules the
        // scatters through a continuation, which in turn releases the partition consumers.
        java.util.function.Supplier<CompletableFuture<Void>> scatter = QueryExecutionScope.propagate(() -> {
            int offset = 0;
            for (int partition = 0; partition < partitionCount; partition++) {
                frame.offsets[partition] = offset;
                for (int chunk = 0; chunk < chunks; chunk++) {
                    int count = cursors[chunk][partition];
                    cursors[chunk][partition] = offset;
                    offset += count;
                }
            }
            frame.offsets[partitionCount] = offset;
            CompletableFuture<?>[] scattered = new CompletableFuture<?>[chunks];
            for (int chunk = 0; chunk < chunks; chunk++) {
                int start = (int) ((long) rowCount * chunk / chunks);
                int end = (int) ((long) rowCount * (chunk + 1) / chunks);
                int[] positions = cursors[chunk];
                scattered[chunk] = CompletableFuture.runAsync(QueryExecutionScope.propagate(() -> {
                    for (int row = start; row < end; row++) {
                        if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                        int partition = partitionForHash(frame.hashes[row], partitionCount);
                        frame.rows[positions[partition]++] = row;
                    }
                }), executor);
            }
            return CompletableFuture.allOf(scattered);
        });
        return CompletableFuture.allOf(work).thenCompose(ignored -> scatter.get());
    }

    void accumulateFrame(PartitionedFrame frame) {
        int start = frame.offsets[partitionIndex];
        int count = frame.offsets[partitionIndex + 1] - start;
        for (int position = 0; position < count; position++) {
            if ((position & 1023) == 0) QueryCancellation.requireCurrentActive();
            int row = frame.rows[start + position];
            int group = findOrCreate(frame.columns, row, frame.hashes[row]);
            accumulateRow(frame.columns, row, group);
        }
    }

    private void computeRowHashes(FrameColumnReader[] input, int rowCount, long[] hashes) {
        computeRowHashes(input, 0, rowCount, hashes);
    }

    private void computeRowHashes(FrameColumnReader[] input, int start, int end, long[] hashes) {
        Arrays.fill(hashes, start, end, HASH_SEED);
        for (int dimension = 0; dimension < dimensionColumns.length; dimension++) {
            QueryCancellation.requireCurrentActive();
            int column = dimensionColumns[dimension];
            if (column < 0) {
                for (int row = start; row < end; row++) {
                    if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                    hashes[row] = mixHash(hashes[row], NULL_KEY_HASH);
                }
                continue;
            }
            FrameColumnReader reader = input[column];
            for (int row = start; row < end; row++) {
                if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                long keyHash = reader.isNullAt(row) ? NULL_KEY_HASH : keyHash(reader, row, dimension);
                hashes[row] = mixHash(hashes[row], keyHash);
            }
        }
        for (int row = start; row < end; row++) {
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
        // Leave 20% of tag slots empty; payload pages do not move when this smaller index grows.
        if ((groupCount + 1L) * 5 > slots.length * 4L) {
            growSlots();
            slot = (int) hash & slotMask;
            while (slots[slot] != EMPTY_SLOT) slot = (slot + 1) & slotMask;
        }
        int group = groupCount++;
        ensurePage(group);
        wordPage(group)[wordBase(group) + hashWordOffset] = packedHash ? hash << 32 : hash;
        copyDimensions(group, input, row);
        slots[slot] = hashTag << Integer.SIZE | (group + 1L);
        return group;
    }

    private void ensurePage(int group) {
        int page = group >>> GROUPS_PER_PAGE_SHIFT;
        if (page == wordPages.length) {
            wordPages = Arrays.copyOf(wordPages, page * 2);
            objectPages = Arrays.copyOf(objectPages, page * 2);
        }
        if (wordPages[page] != null && (page != 0 || (group + 1) * wordsPerGroup <= wordPages[page].length)) return;
        int capacity = page == 0 ? Math.min(GROUPS_PER_PAGE,
                wordPages[0] == null ? 16 : wordPages[0].length / wordsPerGroup * 2) : GROUPS_PER_PAGE;
        wordPages[page] = wordPages[page] == null ? new long[capacity * wordsPerGroup]
                : Arrays.copyOf(wordPages[page], capacity * wordsPerGroup);
        if (objectsPerGroup > 0) {
            objectPages[page] = objectPages[page] == null ? new Object[capacity * objectsPerGroup]
                    : Arrays.copyOf(objectPages[page], capacity * objectsPerGroup);
        }
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

    /** Update every measure while the probed group's contiguous state is hot in cache. */
    private void accumulateRow(FrameColumnReader[] input, int row, int group) {
        long[] words = wordPage(group);
        int base = wordBase(group);
        for (int measure = 0; measure < measureColumns.length; measure++) {
            FrameColumnReader column = input[measureColumns[measure]];
            if (column.isNullAt(row)) continue;
            int flag = measureFlag(measure);
            boolean initialized = hasFlag(words, base, flag);
            int slot = base + measureWordOffsets[measure];
            AggregateOperation function = measureFunctions[measure];
            switch (measureTypes[measure]) {
                case LONG -> {
                    long incoming = column.longValue(row);
                    long previous = initialized ? words[slot] : incoming;
                    words[slot] = !initialized ? incoming : switch (function) {
                        case SUM, COUNT -> Math.addExact(previous, incoming);
                        case MINIMUM -> Math.min(previous, incoming);
                        case MAXIMUM -> Math.max(previous, incoming);
                    };
                }
                case DOUBLE -> {
                    double incoming = column.doubleValue(row);
                    double value = initialized
                            ? function.combine(Double.longBitsToDouble(words[slot]), incoming) : incoming;
                    words[slot] = Double.doubleToRawLongBits(value);
                }
                case STRING -> stringMeasures[measure].accumulate(group, column.stringValue(row), initialized);
                case DECIMAL -> {
                    Object[] objects = objectPage(group);
                    int objectSlot = objectBase(group) + measureObjectOffsets[measure];
                    String name = schema.columnNames().get(measureColumns[measure]);
                    BigDecimal incoming = column.decimalValue(row);
                    SparkValues.requireDecimal(incoming, name);
                    if (!initialized) {
                        objects[objectSlot] = incoming;
                    } else {
                        BigDecimal previous = (BigDecimal) objects[objectSlot];
                        objects[objectSlot] = switch (function) {
                            case SUM, COUNT -> SparkValues.decimalSum(previous, incoming, name);
                            case MINIMUM -> previous.min(incoming);
                            case MAXIMUM -> previous.max(incoming);
                        };
                    }
                }
            }
            if (!initialized) setFlag(words, base, flag);
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
            long storedHash = wordPage(group)[wordBase(group) + hashWordOffset];
            int slot = (int) (packedHash ? storedHash >>> 32 : storedHash) & grownMask;
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
                ? stringMeasures[measure].value(group)
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
