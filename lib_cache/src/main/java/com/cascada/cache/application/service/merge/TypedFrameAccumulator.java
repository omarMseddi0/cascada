package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.IntStream;

import static com.cascada.cache.application.service.merge.TypedFrameAggregator.*;

/**
 * Incrementally merges typed partial frames by their dimensions. Frames must be offered by one
 * producer at a time; {@link #finish()} or {@link #finishWithAverages(List)} is called once after the
 * last frame.
 */
public final class TypedFrameAccumulator {
    /** Two frames overlap routing with aggregation while bounding retained inputs and routing scratch. */
    private static final int MAXIMUM_FRAMES_IN_FLIGHT = 2;
    private static final int NOT_PRESENT = -1;
    private static final String DEFAULT_AVERAGE_INPUT_TYPE = "double";

    private final ResultFrame schema;
    private final List<String> dimensions;
    private final List<String> measures;
    private final ColumnType[] dimensionTypes;
    private final boolean[] timeDimensions;
    private final int[] columnDimensions;
    private final int[] columnMeasures;
    private final List<TypedAggregationGroups> partitions;
    private final Executor partitionExecutor;
    private final List<CompletableFuture<Void>> partitionTails;
    private final Deque<CompletableFuture<Void>> framesInFlight = new ArrayDeque<>();
    // The producer reuses a buffer only after every consumer of that frame has completed.
    private final Deque<PartitionedFrame> routingBuffers = new ArrayDeque<>();

    public TypedFrameAccumulator(ResultFrame inputSchema, List<String> dimensions,
                                 Map<String, AggregateFunction> functions, String timeColumn, int step) {
        this(inputSchema, dimensions, functions, timeColumn, step, 1, Runnable::run);
    }

    /**
     * With {@code partitionCount > 1} and at least one dimension, groups are hash-partitioned and each
     * partition merges on {@code partitionExecutor}. A partition processes frames in arrival order and
     * owns its groups exclusively, so every group is summed in the same order as a single-partition
     * merge and produces bit-identical results.
     */
    public TypedFrameAccumulator(ResultFrame inputSchema, List<String> dimensions,
                                 Map<String, AggregateFunction> functions, String timeColumn, int step,
                                 int partitionCount, Executor partitionExecutor) {
        Optional.of(partitionCount)
                .filter(count -> count > 0)
                .orElseThrow(() -> new IllegalArgumentException("partitionCount must be > 0"));
        this.partitionExecutor = Objects.requireNonNull(partitionExecutor, "partitionExecutor");
        ResultFrameBuilder descriptor = ResultFrame.builder();
        for (String column : inputSchema.columnNames()) descriptor.column(column, inputSchema.columnType(column));
        this.schema = descriptor.build();
        this.dimensions = List.copyOf(dimensions);
        measures = selectMeasures(schema, dimensions, functions);

        int[] dimensionColumns = new int[dimensions.size()];
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
        AggregateOperation[] measureFunctions = new AggregateOperation[measures.size()];
        for (int measure = 0; measure < measures.size(); measure++) {
            String name = measures.get(measure);
            measureColumns[measure] = schema.columnIndex(name);
            measureTypes[measure] = schema.columnType(name);
            // Name normalization and aggregate resolution belong outside the row/measure hot loop.
            measureFunctions[measure] = AggregateOperation.fromFunction(AggregateFunctionResolver.resolve(name, functions));
        }

        columnDimensions = schema.columnNames().stream().mapToInt(this.dimensions::indexOf).toArray();
        columnMeasures = schema.columnNames().stream().mapToInt(measures::indexOf).toArray();

        int effectivePartitionCount = this.dimensions.isEmpty() ? 1 : partitionCount;
        partitions = IntStream.range(0, effectivePartitionCount)
                .mapToObj(partition -> new TypedAggregationGroups(schema, dimensionColumns, dimensionTypes,
                        timeDimensions, step, measureColumns, measureTypes, measureFunctions,
                        partition, effectivePartitionCount))
                .toList();
        partitionTails = new ArrayList<>(Collections.nCopies(effectivePartitionCount,
                CompletableFuture.<Void>completedFuture(null)));
    }

    public void accept(ResultFrame input) {
        QueryCancellation.requireCurrentActive();
        validateCompatibleSchemas(List.of(schema, input));
        if (input.rowCount() == 0) {
            return;
        }
        if (partitions.size() == 1) {
            accumulateFrame(partitions.get(0), input);
            return;
        }
        // Wait before scheduling more work so the number of retained input frames never exceeds
        // MAXIMUM_FRAMES_IN_FLIGHT, even when a partition is slower than its producer.
        PartitionedFrame reusable = null;
        if (framesInFlight.size() == MAXIMUM_FRAMES_IN_FLIGHT) {
            awaitCompletion(framesInFlight.removeFirst());
            reusable = routingBuffers.removeFirst();
        }
        FrameColumnReader[] columns = new FrameColumnReader[input.columnNames().size()];
        Arrays.setAll(columns, input::columnReader);
        PartitionedFrame routedFrame = partitions.get(0).partitionFrame(columns, input.rowCount(), reusable);
        for (int partition = 0; partition < partitions.size(); partition++) {
            TypedAggregationGroups groups = partitions.get(partition);
            partitionTails.set(partition, partitionTails.get(partition)
                    .thenRunAsync(QueryExecutionScope.propagate(() -> groups.accumulateFrame(routedFrame)),
                            partitionExecutor));
        }
        framesInFlight.addLast(allPartitionWork());
        routingBuffers.addLast(routedFrame);
    }

    private static void accumulateFrame(TypedAggregationGroups groups, ResultFrame input) {
        FrameColumnReader[] columns = new FrameColumnReader[input.columnNames().size()];
        Arrays.setAll(columns, input::columnReader);
        groups.accumulateFrame(columns, input.rowCount());
    }

    /** Every merged group, in dimension order, with all ingredient columns of the input schema. */
    public ResultFrame finish() {
        return buildMergedFrame(orderedGroups());
    }

    /**
     * The merged frame with each {@code AVG} alias rebuilt in place of its SUM/COUNT/type ingredients,
     * exactly as {@link SparkAverage#finish} would produce from {@link #finish()}, but computed straight
     * from the group state instead of through an intermediate ingredient frame.
     */
    public ResultFrame finishWithAverages(List<String> averageAliases) {
        long[] orderedGroups = orderedGroups();
        boolean averagesFromGroupState = orderedGroups.length > 0
                && averageAliases.stream().allMatch(this::hasMeasureIngredients);
        return averagesFromGroupState
                ? buildAveragedFrame(orderedGroups, averageAliases)
                : SparkAverage.finish(buildMergedFrame(orderedGroups), averageAliases);
    }

    private boolean hasMeasureIngredients(String alias) {
        String typeColumn = AverageIngredients.typeColumn(alias);
        return measures.contains(AverageIngredients.sumColumn(alias))
                && measures.contains(AverageIngredients.countColumn(alias))
                && measures.contains(typeColumn)
                && schema.columnType(typeColumn) == ColumnType.STRING;
    }

    private ResultFrame buildMergedFrame(long[] orderedGroups) {
        ResultFrameBuilder result = ResultFrame.builder().expectedRows(orderedGroups.length);
        schema.columnNames().forEach(name -> result.column(name, schema.columnType(name)));
        for (long entry : orderedGroups) {
            TypedAggregationGroups groups = partitionOf(entry);
            int group = groupOf(entry);
            for (int column = 0; column < columnDimensions.length; column++) {
                appendColumn(result, groups, group, column);
            }
        }
        return result.build();
    }

    private ResultFrame buildAveragedFrame(long[] orderedGroups, List<String> averageAliases) {
        int columnCount = columnDimensions.length;
        int[] averageCountMeasures = new int[columnCount];
        ColumnType[] averageResultTypes = new ColumnType[columnCount];
        String[] averageInputTypes = new String[columnCount];
        String[] averageAliasesBySumColumn = new String[columnCount];
        boolean[] droppedColumns = new boolean[columnCount];
        Arrays.fill(averageCountMeasures, NOT_PRESENT);
        for (String alias : averageAliases) {
            int sumColumn = schema.columnIndex(AverageIngredients.sumColumn(alias));
            int countColumn = schema.columnIndex(AverageIngredients.countColumn(alias));
            int typeColumn = schema.columnIndex(AverageIngredients.typeColumn(alias));
            String inputType = firstAverageInputType(orderedGroups, columnMeasures[typeColumn]);
            averageAliasesBySumColumn[sumColumn] = alias;
            averageCountMeasures[sumColumn] = columnMeasures[countColumn];
            averageInputTypes[sumColumn] = inputType;
            averageResultTypes[sumColumn] = SparkAverage.resultType(inputType);
            droppedColumns[countColumn] = true;
            droppedColumns[typeColumn] = true;
        }

        int[] outputColumns = IntStream.range(0, columnCount).filter(column -> !droppedColumns[column]).toArray();
        ResultFrameBuilder result = ResultFrame.builder().expectedRows(orderedGroups.length);
        for (int column : outputColumns) {
            if (averageAliasesBySumColumn[column] == null) {
                result.column(schema.columnNames().get(column), schema.columnTypeAt(column));
            } else {
                result.column(averageAliasesBySumColumn[column], averageResultTypes[column]);
            }
        }
        for (long entry : orderedGroups) {
            TypedAggregationGroups groups = partitionOf(entry);
            int group = groupOf(entry);
            for (int column : outputColumns) {
                if (averageAliasesBySumColumn[column] == null) {
                    appendColumn(result, groups, group, column);
                } else {
                    groups.appendAverage(result, group, columnMeasures[column], averageCountMeasures[column],
                            averageResultTypes[column], averageInputTypes[column]);
                }
            }
        }
        return result.build();
    }

    /** {@code typeof(x)} of the first merged group that has one, as {@code SparkAverage} reads it. */
    private String firstAverageInputType(long[] orderedGroups, int typeMeasure) {
        return Arrays.stream(orderedGroups)
                .mapToObj(entry -> partitionOf(entry).stringMeasure(groupOf(entry), typeMeasure))
                .filter(Objects::nonNull)
                .findFirst()
                .map(inputType -> inputType.toLowerCase(Locale.ROOT))
                .orElse(DEFAULT_AVERAGE_INPUT_TYPE);
    }

    private void appendColumn(ResultFrameBuilder result, TypedAggregationGroups groups, int group, int column) {
        int dimension = columnDimensions[column];
        if (dimension >= 0) {
            groups.appendDimension(result, group, dimension, dimensionTypes[dimension], timeDimensions[dimension]);
        } else {
            groups.appendMeasure(result, group, columnMeasures[column]);
        }
    }

    /**
     * Waits for all partition work, then returns every group as {@code partition << 32 | group} in
     * dimension order: each partition is sorted on its own, then the sorted partitions are merged.
     */
    private long[] orderedGroups() {
        QueryCancellation.requireCurrentActive();
        awaitCompletion(allPartitionWork());
        framesInFlight.clear();
        routingBuffers.clear();
        if (partitions.size() == 1) {
            QueryCancellation.requireCurrentActive();
            return Arrays.stream(partitions.get(0).sortedGroupIds(dimensions)).asLongStream().toArray();
        }
        List<CompletableFuture<int[]>> sortedPartitions = partitions.stream()
                .map(groups -> CompletableFuture.supplyAsync(
                        QueryExecutionScope.propagate(() -> groups.sortedGroupIds(dimensions)), partitionExecutor))
                .toList();
        awaitCompletion(CompletableFuture.allOf(sortedPartitions.toArray(CompletableFuture[]::new)));
        return mergeSortedPartitions(sortedPartitions.stream().map(CompletableFuture::join).toList());
    }

    private long[] mergeSortedPartitions(List<int[]> sortedPartitions) {
        long[] merged = new long[sortedPartitions.stream().mapToInt(groupIds -> groupIds.length).sum()];
        int[] cursors = new int[sortedPartitions.size()];
        for (int output = 0; output < merged.length; output++) {
            int nextPartition = NOT_PRESENT;
            for (int partition = 0; partition < cursors.length; partition++) {
                if (cursors[partition] < sortedPartitions.get(partition).length
                        && (nextPartition == NOT_PRESENT || precedes(partition, cursors[partition],
                                nextPartition, cursors[nextPartition], sortedPartitions))) {
                    nextPartition = partition;
                }
            }
            int group = sortedPartitions.get(nextPartition)[cursors[nextPartition]++];
            merged[output] = (long) nextPartition << Integer.SIZE | group;
        }
        return merged;
    }

    private boolean precedes(int partition, int cursor, int otherPartition, int otherCursor, List<int[]> sortedPartitions) {
        return partitions.get(partition).compareWithGroupOf(sortedPartitions.get(partition)[cursor],
                partitions.get(otherPartition), sortedPartitions.get(otherPartition)[otherCursor], dimensions.size()) < 0;
    }

    private TypedAggregationGroups partitionOf(long entry) {
        return partitions.get((int) (entry >>> Integer.SIZE));
    }

    private static int groupOf(long entry) {
        return (int) entry;
    }

    private CompletableFuture<Void> allPartitionWork() {
        return CompletableFuture.allOf(partitionTails.toArray(CompletableFuture[]::new));
    }

    /** Rethrows a partition's own failure, so callers see the same exception as a single-partition merge. */
    private static void awaitCompletion(CompletableFuture<?> work) {
        try {
            work.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException partitionFailure) throw partitionFailure;
            if (failure.getCause() instanceof Error partitionError) throw partitionError;
            throw failure;
        }
    }
}
