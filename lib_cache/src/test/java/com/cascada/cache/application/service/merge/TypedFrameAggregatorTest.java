package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnNames;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
    @ValueSource(ints = {2, 3, 8})
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
    @ValueSource(ints = {1, 4})
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
}
