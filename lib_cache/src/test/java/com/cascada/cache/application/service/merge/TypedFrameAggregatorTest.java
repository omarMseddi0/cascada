package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.domain.frame.ColumnNames;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypedFrameAggregatorTest {

    @Test
    void keepsOneGlobalGroupAcrossEmptyAndNonEmptyIncrementalFramesWithNullTypedMeasures() {
        ResultFrame empty = ResultFrame.builder()
                .column("SUM(long_value)", ColumnType.LONG)
                .column("SUM(double_value)", ColumnType.DOUBLE)
                .column("MIN(label)", ColumnType.STRING)
                .column("SUM(decimal_value)", ColumnType.DECIMAL)
                .build();
        ResultFrame first = ResultFrame.builder()
                .column("SUM(long_value)", ColumnType.LONG)
                .column("SUM(double_value)", ColumnType.DOUBLE)
                .column("MIN(label)", ColumnType.STRING)
                .column("SUM(decimal_value)", ColumnType.DECIMAL)
                .row(10L, 1.5, "zeta", new BigDecimal("2.50"))
                .row(null, null, null, null)
                .build();
        ResultFrame second = ResultFrame.builder()
                .column("SUM(long_value)", ColumnType.LONG)
                .column("SUM(double_value)", ColumnType.DOUBLE)
                .column("MIN(label)", ColumnType.STRING)
                .column("SUM(decimal_value)", ColumnType.DECIMAL)
                .row(3L, -0.5, "alpha", new BigDecimal("1.25"))
                .row(2L, 2.0, null, null)
                .build();
        Map<String, AggregateFunction> functions = Map.of(
                "SUM(long_value)", AggregateFunction.SUM,
                "SUM(double_value)", AggregateFunction.SUM,
                "MIN(label)", AggregateFunction.MINIMUM,
                "SUM(decimal_value)", AggregateFunction.SUM);

        TypedFrameAccumulator incremental = new TypedFrameAccumulator(empty, List.of(), functions, "ts", 300);
        incremental.accept(empty);
        incremental.accept(first);
        incremental.accept(empty);
        incremental.accept(second);
        ResultFrame actual = incremental.finish();
        ResultFrame bulk = TypedFrameAggregator.aggregate(List.of(empty, first, empty, second), List.of(),
                functions, "ts", 300);

        assertThat(actual.rows()).isEqualTo(bulk.rows());
        assertThat(actual.rowCount()).isEqualTo(1);
        assertThat(actual.rows().get(0))
                .containsEntry("SUM(long_value)", 15L)
                .containsEntry("SUM(double_value)", 3.0)
                .containsEntry("MIN(label)", "alpha")
                .containsEntry("SUM(decimal_value)", new BigDecimal("3.75"));
    }

    @Test
    void keepsSparkNullNanSignedZeroAndScaleInsensitiveGroupingSemantics() {
        ResultFrame first = ResultFrame.builder()
                .column("d", ColumnType.DOUBLE).column("decimal_id", ColumnType.DECIMAL)
                .column("SUM(x)", ColumnType.LONG)
                .row(null, new BigDecimal("1.0"), 1L)
                .row(0.0, new BigDecimal("1.0"), 2L)
                .row(-0.0, new BigDecimal("1.00"), 3L)
                .row(Double.NaN, new BigDecimal("1.0"), 4L)
                .build();
        ResultFrame second = ResultFrame.builder()
                .column("d", ColumnType.DOUBLE).column("decimal_id", ColumnType.DECIMAL)
                .column("SUM(x)", ColumnType.LONG)
                .row(Double.NaN, new BigDecimal("1.00"), 5L)
                .build();

        ResultFrame merged = TypedFrameAggregator.aggregate(List.of(first, second), List.of("d", "decimal_id"),
                Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300);

        // Spark 3.0+ normalises grouping keys: -0.0 groups with 0.0 and every NaN is one group
        // (Seq(-0.0, 0.0).toDF("d").groupBy("d").count() returns [(0.0, 2)]).
        assertThat(merged.rowCount()).isEqualTo(3);
        assertThat(merged.rows()).extracting(row -> row.get("SUM(x)"))
                .containsExactly(1L, 5L, 9L);
        assertThat(merged.rows().get(0).get("d")).isNull();
        assertThat(Double.doubleToLongBits((Double) merged.rows().get(1).get("d")))
                .isEqualTo(Double.doubleToLongBits(0.0));
        assertThat((Double) merged.rows().get(2).get("d")).isNaN();
        // The first row representing a numeric decimal group keeps its original scale.
        assertThat(merged.rows().get(1).get("decimal_id")).isEqualTo(new BigDecimal("1.0"));
        assertThat(merged.rows().get(2).get("decimal_id")).isEqualTo(new BigDecimal("1.0"));
    }

    @Test
    void preservesFrameAndRowOrderForFloatingPointAccumulationAndFloorsNegativeTime() {
        ResultFrame first = ResultFrame.builder()
                .column("ts", ColumnType.LONG).column("SUM(x)", ColumnType.DOUBLE)
                .row(-301L, 1.0e16).row(-301L, -1.0e16).row(-301L, 1.0)
                .row(-300L, 2.0).build();
        ResultFrame second = ResultFrame.builder()
                .column("ts", ColumnType.LONG).column("SUM(x)", ColumnType.DOUBLE)
                .row(299L, 3.0).row(0L, 4.0).build();

        ResultFrame merged = TypedFrameAggregator.aggregate(List.of(first, second), List.of("ts"),
                Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300);

        assertThat(merged.rowCount()).isEqualTo(3);
        assertThat(merged.rows().get(0).get("ts")).isEqualTo(-600L);
        assertThat(merged.rows().get(0).get("SUM(x)")).isEqualTo(1.0);
        assertThat(merged.rows().get(1).get("ts")).isEqualTo(-300L);
        assertThat(merged.rows().get(1).get("SUM(x)")).isEqualTo(2.0);
        assertThat(merged.rows().get(2).get("ts")).isEqualTo(0L);
        assertThat(merged.rows().get(2).get("SUM(x)")).isEqualTo(7.0);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 8, 10, 100})
    void partitionedMergeEqualsSinglePartitionMergeForEveryKeyAndMeasureType(int partitionCount) {
        List<ResultFrame> frames = mixedTypeFrames();
        Map<String, AggregateFunction> functions = Map.of(
                "SUM(order_sensitive)", AggregateFunction.SUM,
                "SUM(quantity)", AggregateFunction.SUM,
                "MIN(label)", AggregateFunction.MINIMUM,
                "SUM(amount)", AggregateFunction.SUM);
        List<String> dimensions = List.of("long_key", "string_key", "double_key", "decimal_key");

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            TypedFrameAccumulator single = new TypedFrameAccumulator(frames.get(0), dimensions, functions, "ts", 300);
            TypedFrameAccumulator partitioned = new TypedFrameAccumulator(frames.get(0), dimensions, functions, "ts", 300,
                    partitionCount, executor);
            frames.forEach(single::accept);
            frames.forEach(partitioned::accept);

            assertThat(partitioned.finish().rows()).isEqualTo(single.finish().rows());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 10})
    void averagesFromGroupStateEqualSparkAverageOverTheIngredientFrame(int partitionCount) {
        String doubleAverage = "average_value";
        String decimalAverage = "average_amount";
        List<ResultFrame> frames = averageIngredientFrames(doubleAverage, decimalAverage);
        // Keys use the merge's normalized column identity, as FrameMergeService registers AVG ingredients.
        Map<String, AggregateFunction> functions = Map.of(
                ColumnNames.identity(AverageIngredients.sumColumn(doubleAverage)), AggregateFunction.SUM,
                ColumnNames.identity(AverageIngredients.countColumn(doubleAverage)), AggregateFunction.COUNT,
                ColumnNames.identity(AverageIngredients.typeColumn(doubleAverage)), AggregateFunction.MINIMUM,
                ColumnNames.identity(AverageIngredients.sumColumn(decimalAverage)), AggregateFunction.SUM,
                ColumnNames.identity(AverageIngredients.countColumn(decimalAverage)), AggregateFunction.COUNT,
                ColumnNames.identity(AverageIngredients.typeColumn(decimalAverage)), AggregateFunction.MINIMUM);
        List<String> averages = List.of(doubleAverage, decimalAverage);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            TypedFrameAccumulator ingredients = new TypedFrameAccumulator(frames.get(0), List.of("device"), functions,
                    "ts", 300, partitionCount, executor);
            TypedFrameAccumulator averaged = new TypedFrameAccumulator(frames.get(0), List.of("device"), functions,
                    "ts", 300, partitionCount, executor);
            frames.forEach(ingredients::accept);
            frames.forEach(averaged::accept);

            ResultFrame expected = SparkAverage.finish(ingredients.finish(), averages);
            ResultFrame actual = averaged.finishWithAverages(averages);

            assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
            assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
            assertThat(actual.rows()).isEqualTo(expected.rows());
        }
    }

    @Test
    void partitionFailureSurfacesAsTheOriginalMergeException() {
        ResultFrame largeCount = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG)
                .row(1L, Long.MAX_VALUE).row(2L, 5L).build();
        ResultFrame overflowingCount = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG)
                .row(1L, 1L).build();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            TypedFrameAccumulator partitioned = new TypedFrameAccumulator(largeCount, List.of("device"),
                    Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 4, executor);
            partitioned.accept(largeCount);
            partitioned.accept(overflowingCount);

            assertThatThrownBy(partitioned::finish).isInstanceOf(ArithmeticException.class);
        }
    }

    @Test
    void queuedPartitionWorkInheritsQueryCancellation() {
        ResultFrame frame = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG)
                .row(1L, 2L).row(2L, 3L).build();
        QueuedExecutor executor = new QueuedExecutor();
        QueryCancellation cancellation = new QueryCancellation();
        TypedFrameAccumulator partitioned = new TypedFrameAccumulator(frame, List.of("device"),
                Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 2, executor);

        try (QueryExecutionScope ignored = new QueryExecutionScope(cancellation)) {
            partitioned.accept(frame);
        }
        cancellation.cancel();
        executor.runAll();

        assertThatThrownBy(partitioned::finish).isInstanceOf(CancellationException.class);
    }

    @Test
    void partitionedMergeBackpressuresBeforeAcceptingMoreThanTwoFrames() throws Exception {
        ResultFrame frame = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG)
                .row(1L, 2L).row(2L, 3L).build();
        QueuedExecutor executor = new QueuedExecutor();
        TypedFrameAccumulator partitioned = new TypedFrameAccumulator(frame, List.of("device"),
                Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 2, executor);
        for (int index = 0; index < 2; index++) partitioned.accept(frame);

        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = Thread.ofVirtual().start(() -> {
            started.countDown();
            try {
                partitioned.accept(frame);
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                finished.countDown();
            }
        });

        assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (finished.getCount() != 0 && producer.getState() != Thread.State.WAITING
                && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertThat(producer.getState()).isEqualTo(Thread.State.WAITING);
        executor.runAll();
        assertThat(finished.await(2, TimeUnit.SECONDS)).isTrue();
        producer.join();
        executor.runAll();
        executor.runInline();

        assertThat(failure.get()).isNull();
        assertThat(partitioned.finish().rows()).containsExactly(
                Map.of("device", 1L, "SUM(x)", 6L), Map.of("device", 2L, "SUM(x)", 9L));
    }

    @Test
    void growsPagedGroupsAndHashSlotsPastSixtyFiveThousandGroups() {
        int keyCount = 70_000;
        ResultFrameBuilder ascendingBuilder = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG);
        ResultFrameBuilder descendingBuilder = ResultFrame.builder()
                .column("device", ColumnType.LONG).column("SUM(x)", ColumnType.LONG);
        for (int key = 0; key < keyCount; key++) ascendingBuilder.row((long) key, (long) key + 1L);
        for (int key = keyCount - 1; key >= 0; key--) descendingBuilder.row((long) key, 2L * (key + 1L));
        ResultFrame ascending = ascendingBuilder.build();
        ResultFrame descending = descendingBuilder.build();
        Map<String, AggregateFunction> functions = Map.of("SUM(x)", AggregateFunction.SUM);

        TypedFrameAccumulator single = new TypedFrameAccumulator(ascending, List.of("device"), functions, "ts", 300);
        single.accept(ascending);
        single.accept(descending);
        ResultFrame expected = single.finish();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            TypedFrameAccumulator partitioned = new TypedFrameAccumulator(ascending, List.of("device"), functions,
                    "ts", 300, 4, executor);
            partitioned.accept(ascending);
            partitioned.accept(descending);
            ResultFrame actual = partitioned.finish();

            assertThat(expected.rowCount()).isEqualTo(keyCount);
            assertThat(actual.rowCount()).isEqualTo(keyCount);
            for (int row = 0; row < keyCount; row++) {
                long key = row;
                long expectedSum = 3L * (key + 1L);
                if (expected.longAt(row, 0) != key || expected.longAt(row, 1) != expectedSum
                        || actual.longAt(row, 0) != key || actual.longAt(row, 1) != expectedSum) {
                    throw new AssertionError("unexpected merge result at sorted row " + row);
                }
            }
        }
    }

    @Test
    void reusesRoutingBuffersAcrossGrowingShrinkingEmptyAndSkewedFrames() {
        int[] sizes = {1_100, 3, 0, 2_048, 1, 17, 4_097, 2, 33, 500, 1, 7};
        var expected = new java.util.TreeMap<Long, Long>();
        try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
            ResultFrame schema = ResultFrame.builder().column("device", ColumnType.LONG)
                    .column("SUM(x)", ColumnType.LONG).build();
            TypedFrameAccumulator accumulator = new TypedFrameAccumulator(schema, List.of("device"),
                    Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 4, executor);
            for (int frame = 0; frame < sizes.length; frame++) {
                ResultFrameBuilder input = ResultFrame.builder().column("device", ColumnType.LONG)
                        .column("SUM(x)", ColumnType.LONG);
                for (int row = 0; row < sizes[frame]; row++) {
                    long key = frame % 2 == 0 ? row % 37 : 0;
                    long value = row + frame + 1L;
                    input.row(key, value);
                    expected.merge(key, value, Long::sum);
                }
                accumulator.accept(input.build());
            }
            ResultFrame actual = accumulator.finish();
            assertThat(actual.rowCount()).isEqualTo(expected.size());
            int row = 0;
            for (var entry : expected.entrySet()) {
                assertThat(actual.longAt(row, 0)).isEqualTo(entry.getKey());
                assertThat(actual.longAt(row, 1)).isEqualTo(entry.getValue());
                row++;
            }
        }
    }

    @Test
    void parallelHashScatterKeepsTheExactPerKeyUpdateOrderAcrossChunksAndFrames() {
        double[] expected = new double[103];
        ResultFrame schema = ResultFrame.builder().column("key", ColumnType.LONG)
                .column("SUM(x)", ColumnType.DOUBLE).build();
        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
            TypedFrameAccumulator accumulator = new TypedFrameAccumulator(schema, List.of("key"),
                    Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 10, executor);
            for (int size : new int[]{300_001, 271_119, 17, 320_001}) {
                ResultFrameBuilder builder = ResultFrame.builder().expectedRows(size)
                        .column("key", ColumnType.LONG).column("SUM(x)", ColumnType.DOUBLE);
                for (int row = 0; row < size; row++) {
                    int key = row % expected.length;
                    double value = switch (row % 3) { case 0 -> 1.0e16; case 1 -> -1.0e16; default -> 1.0; };
                    builder.appendLong(key).appendDouble(value);
                    expected[key] += value;
                }
                accumulator.accept(builder.build());
            }
            ResultFrame actual = accumulator.finish();
            assertThat(actual.rowCount()).isEqualTo(expected.length);
            for (int key = 0; key < expected.length; key++) {
                assertThat(actual.longAt(key, 0)).isEqualTo(key);
                assertThat(Double.doubleToRawLongBits(actual.doubleAt(key, 1)))
                        .isEqualTo(Double.doubleToRawLongBits(expected[key]));
            }
        }
    }

    @Test
    void parallelFusedAverageOutputMatchesSingleWorkerForNullsAndDecimals() {
        String doubleAlias = "davg", decimalAlias = "money_avg";
        ResultFrameBuilder input = ResultFrame.builder().expectedRows(70_003)
                .column("device", ColumnType.LONG)
                .column(AverageIngredients.sumColumn(doubleAlias), ColumnType.DOUBLE)
                .column(AverageIngredients.countColumn(doubleAlias), ColumnType.LONG)
                .column(AverageIngredients.typeColumn(doubleAlias), ColumnType.STRING)
                .column(AverageIngredients.sumColumn(decimalAlias), ColumnType.DECIMAL)
                .column(AverageIngredients.countColumn(decimalAlias), ColumnType.LONG)
                .column(AverageIngredients.typeColumn(decimalAlias), ColumnType.STRING);
        for (int row = 0; row < 70_003; row++) {
            long count = row % 7 == 0 ? 0 : 2;
            input.row((long) row, count == 0 ? null : (row + 0.5) * count, count, "DOUBLE",
                    count == 0 ? null : BigDecimal.valueOf(row, 2).multiply(BigDecimal.valueOf(count)),
                    count, "decimal(10,2)");
        }
        ResultFrame frame = input.build();
        Map<String, AggregateFunction> functions = Map.of(
                ColumnNames.identity(AverageIngredients.typeColumn(doubleAlias)), AggregateFunction.MINIMUM,
                ColumnNames.identity(AverageIngredients.typeColumn(decimalAlias)), AggregateFunction.MINIMUM);
        TypedFrameAccumulator single = new TypedFrameAccumulator(frame, List.of("device"), functions, "ts", 300);
        single.accept(frame);
        single.accept(frame);
        ResultFrame expected = single.finishWithAverages(List.of(doubleAlias, decimalAlias));
        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
            TypedFrameAccumulator parallel = new TypedFrameAccumulator(frame, List.of("device"), functions,
                    "ts", 300, 10, executor);
            parallel.accept(frame);
            parallel.accept(frame);
            ResultFrame actual = parallel.finishWithAverages(List.of(doubleAlias, decimalAlias));
            assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
            assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
            assertThat(actual.rowCount()).isEqualTo(expected.rowCount());
            for (int row = 0; row < actual.rowCount(); row++) {
                for (int column = 0; column < actual.columnNames().size(); column++) {
                    assertThat(actual.valueAt(row, column)).isEqualTo(expected.valueAt(row, column));
                }
            }
        }
    }

    @Test
    void largeFrameAcceptanceDoesNotBlockAWorkerAwaitingItsOwnRoutingTasks() throws Exception {
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(270_000)
                .column("key", ColumnType.LONG).column("SUM(x)", ColumnType.LONG);
        for (int row = 0; row < 270_000; row++) builder.appendLong(row % 3).appendLong(1);
        ResultFrame frame = builder.build();
        try (ExecutorService executor = Executors.newSingleThreadExecutor()) {
            TypedFrameAccumulator accumulator = new TypedFrameAccumulator(frame, List.of("key"),
                    Map.of("SUM(x)", AggregateFunction.SUM), "ts", 300, 10, executor);
            executor.submit(() -> accumulator.accept(frame)).get(5, TimeUnit.SECONDS);
            ResultFrame result = accumulator.finish();
            assertThat(result.rowCount()).isEqualTo(3);
            for (int row = 0; row < 3; row++) {
                assertThat(result.longAt(row, 0)).isEqualTo(row);
                assertThat(result.longAt(row, 1)).isEqualTo(90_000);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {31, 32, 33, 63, 64, 65, 96, 97})
    void compactHashPreservesValidityAcrossBitBoundariesAndTableGrowth(int flagCount) {
        ResultFrameBuilder input = ResultFrame.builder().expectedRows(2_500).column("key", ColumnType.LONG);
        for (int column = 1; column < flagCount; column++) input.column("SUM(x" + column + ")", ColumnType.LONG);
        for (int row = 0; row < 2_500; row++) {
            input.appendLong(row);
            for (int column = 1; column < flagCount; column++) {
                if ((row + column) % 13 == 0) input.appendNull(); else input.appendLong(row + column);
            }
        }
        ResultFrame frame = input.build();
        TypedFrameAccumulator accumulator = new TypedFrameAccumulator(frame, List.of("key"), Map.of(), "ts", 300);
        accumulator.accept(frame);accumulator.accept(frame);
        ResultFrame result = accumulator.finish();
        assertThat(result.rowCount()).isEqualTo(2_500);
        for (int row = 0; row < 2_500; row++) {
            if (result.longAt(row, 0) != row) throw new AssertionError("wrong key");
            for (int column = 1; column < flagCount; column++) {
                boolean missing = (row + column) % 13 == 0;
                if (result.isNullAt(row, column) != missing
                        || (!missing && result.longAt(row, column) != 2L * (row + column))) {
                    throw new AssertionError("wrong validity/value at " + row + "," + column);
                }
            }
        }
    }

    @Test
    void oneHundredPartitionsPreserveResultsWithAnEightThreadCpuBudget() {
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(70_003)
                .column("key", ColumnType.LONG).column("SUM(x)", ColumnType.LONG);
        for (int row = 0; row < 70_003; row++) builder.appendLong(row).appendLong(row + 1L);
        ResultFrame input = builder.build();
        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            TypedFrameAccumulator accumulator = new TypedFrameAccumulator(input, List.of("key"),
                    Map.of(), "ts", 300, 100, executor);
            accumulator.accept(input);accumulator.accept(input);accumulator.accept(input);
            ResultFrame result = accumulator.finish();
            assertThat(result.rowCount()).isEqualTo(70_003);
            for (int row = 0; row < result.rowCount(); row++) {
                if (result.longAt(row, 0) != row || result.longAt(row, 1) != 3L * (row + 1)) {
                    throw new AssertionError("wrong 100-partition result at " + row);
                }
            }
        }
    }

    private static List<ResultFrame> mixedTypeFrames() {
        Random random = new Random(7);
        String[] labels = {"alpha", "beta", "gamma", "Ωmega", "beta"};
        Double[] doubleKeys = {null, 0.0, -0.0, Double.NaN, 1.5};
        BigDecimal[] decimalKeys = {null, new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("2.5")};
        return IntStream.range(0, 12).mapToObj(frameIndex -> {
            ResultFrameBuilder builder = ResultFrame.builder()
                    .column("long_key", ColumnType.LONG).column("string_key", ColumnType.STRING)
                    .column("double_key", ColumnType.DOUBLE).column("decimal_key", ColumnType.DECIMAL)
                    .column("SUM(order_sensitive)", ColumnType.DOUBLE).column("SUM(quantity)", ColumnType.LONG)
                    .column("MIN(label)", ColumnType.STRING).column("SUM(amount)", ColumnType.DECIMAL);
            IntStream.range(0, 400).forEach(row -> builder.row(
                    random.nextInt(9) == 0 ? null : (long) random.nextInt(40),
                    "key-" + random.nextInt(6),
                    doubleKeys[random.nextInt(doubleKeys.length)],
                    decimalKeys[random.nextInt(decimalKeys.length)],
                    random.nextInt(5) == 0 ? null : (random.nextBoolean() ? 1.0e16 : -1.0e16) + random.nextInt(3),
                    random.nextInt(7) == 0 ? null : (long) random.nextInt(1000),
                    random.nextInt(4) == 0 ? null : labels[random.nextInt(labels.length)],
                    random.nextInt(6) == 0 ? null : new BigDecimal(random.nextInt(10_000)).movePointLeft(2)));
            return builder.build();
        }).toList();
    }

    private static List<ResultFrame> averageIngredientFrames(String doubleAverage, String decimalAverage) {
        Random random = new Random(11);
        return IntStream.range(0, 6).mapToObj(frameIndex -> {
            ResultFrameBuilder builder = ResultFrame.builder()
                    .column("device", ColumnType.LONG)
                    .column(AverageIngredients.sumColumn(doubleAverage), ColumnType.DOUBLE)
                    .column(AverageIngredients.countColumn(doubleAverage), ColumnType.LONG)
                    .column(AverageIngredients.typeColumn(doubleAverage), ColumnType.STRING)
                    .column(AverageIngredients.sumColumn(decimalAverage), ColumnType.DECIMAL)
                    .column(AverageIngredients.countColumn(decimalAverage), ColumnType.LONG)
                    .column(AverageIngredients.typeColumn(decimalAverage), ColumnType.STRING);
            IntStream.range(0, 200).forEach(row -> {
                long count = random.nextInt(4);
                builder.row((long) random.nextInt(30),
                        count == 0 ? null : random.nextDouble() * 100 * count, count, "DOUBLE",
                        count == 0 ? null : new BigDecimal(random.nextInt(100_000)).movePointLeft(2), count,
                        "decimal(10,2)");
            });
            return builder.build();
        }).toList();
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> queued = new ArrayDeque<>();
        private boolean inline;

        @Override
        public void execute(Runnable command) {
            synchronized (queued) {
                if (inline) {
                    command.run();
                } else {
                    queued.add(command);
                }
            }
        }

        private void runAll() {
            while (true) {
                Runnable next;
                synchronized (queued) {
                    next = queued.poll();
                }
                if (next == null) return;
                next.run();
            }
        }

        private void runInline() {
            synchronized (queued) {
                inline = true;
            }
        }
    }
}
