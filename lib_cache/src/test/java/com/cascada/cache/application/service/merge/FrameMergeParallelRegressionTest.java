package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.service.FrameMergeService;
import com.cascada.cache.application.service.IncrementalFrameMerge;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.merge.AverageIngredients;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Differential checks through the configured cached-frame merge service, including reconstruction. */
class FrameMergeParallelRegressionTest {

    @Test
    void configuredWorkersKeepMixedNullableKeysAndNonFiniteMeasuresIdentical() {
        QueryMetadata metadata = QueryMetadata.timeSeries(300).withMeasureAggregates(Map.of(
                "SUM(total)", AggregateFunction.SUM,
                "MAX(peak)", AggregateFunction.MAXIMUM,
                "MIN(label)", AggregateFunction.MINIMUM,
                "SUM(measure)", AggregateFunction.SUM,
                "SUM(amount)", AggregateFunction.SUM));
        CanonicalQueryObject query = query(metadata,
                List.of("ts", "tenant", "region", "double_key", "decimal_key"),
                List.of("SUM(total)", "MAX(peak)", "MIN(label)", "SUM(measure)", "SUM(amount)"));

        ResultFrame first = mixedFrame(
                new Object[]{-1L, null, "north", 0.0, new BigDecimal("1.0"), 5L, null, "zeta",
                        Double.POSITIVE_INFINITY, new BigDecimal("1.20")},
                new Object[]{-300L, null, "north", -0.0, new BigDecimal("1.00"), 7L, 9.0, "alpha",
                        Double.NEGATIVE_INFINITY, new BigDecimal("2.30")},
                new Object[]{0L, 2L, null, Double.NaN, new BigDecimal("2.5"), 1L, 3.0, null,
                        Double.NaN, null});
        ResultFrame empty = mixedFrame();
        ResultFrame second = mixedFrame(
                new Object[]{-299L, null, "north", -0.0, new BigDecimal("1.00"), null, 4.0, "beta",
                        2.0, new BigDecimal("0.50")},
                new Object[]{150L, 2L, null, Double.NaN, new BigDecimal("2.50"), 8L, 4.0, null,
                        5.0, null},
                new Object[]{301L, null, "north", Double.NaN, new BigDecimal("2.5"), 4L, null, "gamma",
                        Double.NaN, new BigDecimal("1.00")});
        List<ResultFrame> frames = List.of(first, empty, second);

        ResultFrame expected = mergeWithWorkers(1, frames, query);
        for (int workers : List.of(4, 8)) {
            ResultFrame actual = mergeWithWorkers(workers, frames, query);
            assertSameFrame(actual, expected);
        }

        assertThat(expected.rowCount()).isEqualTo(3);
        assertThat(expected.rows()).extracting(row -> row.get("ts")).containsExactly(-300L, 0L, 300L);
        Map<String, Object> negativeBucket = expected.rows().get(0);
        assertThat(negativeBucket)
                .containsEntry("tenant", null)
                .containsEntry("SUM(total)", 12L)
                .containsEntry("MAX(peak)", 9.0)
                .containsEntry("MIN(label)", "alpha")
                .containsEntry("SUM(amount)", new BigDecimal("4.00"));
        assertThat((Double) negativeBucket.get("SUM(measure)")).isNaN();

        Map<String, Object> nanKey = expected.rows().get(1);
        assertThat((Double) nanKey.get("double_key")).isNaN();
        assertThat(nanKey).containsEntry("SUM(total)", 9L).containsEntry("MAX(peak)", 4.0);
        assertThat(nanKey.get("MIN(label)")).isNull();
        assertThat((Double) nanKey.get("SUM(measure)")).isNaN();
    }

    @Test
    void configuredWorkersReconstructWeightedDoubleAndDecimalAverages() {
        String doubleAverage = "weighted_mean";
        String decimalAverage = "weighted_amount";
        QueryMetadata metadata = QueryMetadata.globalAggregate().withMeasureAggregates(Map.of(
                doubleAverage, AggregateFunction.AVERAGE,
                decimalAverage, AggregateFunction.AVERAGE));
        CanonicalQueryObject query = query(metadata, List.of("device"),
                List.of(AverageIngredients.sumColumn(doubleAverage), AverageIngredients.countColumn(doubleAverage),
                        AverageIngredients.typeColumn(doubleAverage), AverageIngredients.sumColumn(decimalAverage),
                        AverageIngredients.countColumn(decimalAverage), AverageIngredients.typeColumn(decimalAverage)));

        List<ResultFrame> frames = List.of(
                averageFrame(
                        new Object[]{"sensor", 10.0, 2L, "DOUBLE", new BigDecimal("1.00"), 2L, "decimal(10,2)"},
                        new Object[]{"all_null", null, 0L, "DOUBLE", null, 0L, "decimal(10,2)"},
                        new Object[]{null, 3.0, 1L, "DOUBLE", new BigDecimal("0.25"), 1L, "decimal(10,2)"}),
                averageFrame(
                        new Object[]{"sensor", 100.0, 10L, "DOUBLE", new BigDecimal("12.00"), 3L, "decimal(10,2)"},
                        new Object[]{"all_null", null, 0L, "DOUBLE", null, 0L, "decimal(10,2)"},
                        new Object[]{null, 7.0, 3L, "DOUBLE", new BigDecimal("0.75"), 3L, "decimal(10,2)"}));

        ResultFrame expected = mergeWithWorkers(1, frames, query);
        for (int workers : List.of(4, 8)) assertSameFrame(mergeWithWorkers(workers, frames, query), expected);

        assertThat(expected.columnNames()).containsExactly("device", doubleAverage, decimalAverage);
        Map<String, Object> sensor = expected.rows().stream()
                .filter(row -> Objects.equals(row.get("device"), "sensor")).findFirst().orElseThrow();
        assertThat((Double) sensor.get(doubleAverage)).isEqualTo(110.0 / 12.0);
        assertThat(((BigDecimal) sensor.get(decimalAverage)).compareTo(new BigDecimal("2.6"))).isZero();
        Map<String, Object> allNull = expected.rows().stream()
                .filter(row -> Objects.equals(row.get("device"), "all_null")).findFirst().orElseThrow();
        assertThat(allNull.get(doubleAverage)).isNull();
        assertThat(allNull.get(decimalAverage)).isNull();
        Map<String, Object> nullDevice = expected.rows().stream()
                .filter(row -> row.get("device") == null).findFirst().orElseThrow();
        assertThat((Double) nullDevice.get(doubleAverage)).isEqualTo(2.5);
        assertThat(((BigDecimal) nullDevice.get(decimalAverage)).compareTo(new BigDecimal("0.25"))).isZero();
    }

    @Test
    void seededMultiFrameSkewKeepsFloatingPointUpdateAndOutputOrderAcrossWorkers() {
        QueryMetadata metadata = QueryMetadata.timeSeries(300).withMeasureAggregates(Map.of(
                "SUM(value)", AggregateFunction.SUM,
                "SUM(events)", AggregateFunction.SUM));
        CanonicalQueryObject query = query(metadata, List.of("ts", "device"), List.of("SUM(value)", "SUM(events)"));
        List<ResultFrame> frames = skewedFrames();

        ResultFrame expected = mergeWithWorkers(1, frames, query);
        for (int workers : List.of(4, 8)) assertSameFrame(mergeWithWorkers(workers, frames, query), expected);

        assertThat(expected.rowCount()).isGreaterThan(1_000);
        for (int row = 1; row < expected.rowCount(); row++) {
            long priorTime = expected.longAt(row - 1, expected.columnIndex("ts"));
            long time = expected.longAt(row, expected.columnIndex("ts"));
            long priorDevice = expected.longAt(row - 1, expected.columnIndex("device"));
            long device = expected.longAt(row, expected.columnIndex("device"));
            assertThat(time > priorTime || (time == priorTime && device > priorDevice)).isTrue();
        }

        Map<String, Object> hotGroup = expected.rows().stream()
                .filter(row -> Objects.equals(row.get("device"), 0L) && Objects.equals(row.get("ts"), 0L))
                .findFirst().orElseThrow();
        double serialTotal = 0.0;
        long serialEvents = 0L;
        for (ResultFrame frame : frames) {
            for (int row = 0; row < frame.rowCount(); row++) {
                if (Math.floorDiv(frame.longAt(row, frame.columnIndex("ts")), 300L) * 300L == 0L
                        && frame.longAt(row, frame.columnIndex("device")) == 0L) {
                    serialTotal += frame.doubleAt(row, frame.columnIndex("SUM(value)"));
                    serialEvents += frame.longAt(row, frame.columnIndex("SUM(events)"));
                }
            }
        }
        assertThat(Double.doubleToRawLongBits((Double) hotGroup.get("SUM(value)")))
                .isEqualTo(Double.doubleToRawLongBits(serialTotal));
        assertThat(hotGroup.get("SUM(events)")).isEqualTo(serialEvents);
    }

    @Test
    void configuredWorkersKeepDeferredOverflowAndCancellationFailuresObservable() {
        QueryMetadata metadata = QueryMetadata.globalAggregate().withMeasureAggregates(
                Map.of("SUM(total)", AggregateFunction.SUM));
        CanonicalQueryObject query = query(metadata, List.of("device"), List.of("SUM(total)"));
        ResultFrame high = ResultFrame.builder().column("device", ColumnType.LONG)
                .column("SUM(total)", ColumnType.LONG).row(1L, Long.MAX_VALUE).build();
        ResultFrame increment = ResultFrame.builder().column("device", ColumnType.LONG)
                .column("SUM(total)", ColumnType.LONG).row(1L, 1L).build();

        for (int workers : List.of(1, 4, 8)) {
            assertThatThrownBy(() -> mergeWithWorkers(workers, List.of(high, increment), query))
                    .isInstanceOf(ArithmeticException.class).hasMessage("long overflow");
        }

        QueuedExecutor executor = new QueuedExecutor();
        FrameMergeService service = new FrameMergeService(300, "ts", 4, executor);
        IncrementalFrameMerge merge = service.incremental(query);
        QueryCancellation cancellation = new QueryCancellation();
        ResultFrame valid = ResultFrame.builder().column("device", ColumnType.LONG)
                .column("SUM(total)", ColumnType.LONG).row(1L, 2L).row(2L, 3L).build();
        try (QueryExecutionScope ignored = new QueryExecutionScope(cancellation)) {
            merge.add(valid);
        }
        cancellation.cancel();
        executor.runAll();
        assertThatThrownBy(merge::finish).isInstanceOf(CancellationException.class);
    }

    private static ResultFrame mergeWithWorkers(int workerCount, List<ResultFrame> frames,
                                               CanonicalQueryObject query) {
        if (workerCount == 1) {
            return new FrameMergeService(300, "ts", 1, Runnable::run).mergeAndReconstruct(frames, query);
        }
        try (ExecutorService executor = Executors.newFixedThreadPool(workerCount)) {
            return new FrameMergeService(300, "ts", workerCount, executor).mergeAndReconstruct(frames, query);
        }
    }

    private static void assertSameFrame(ResultFrame actual, ResultFrame expected) {
        assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
        assertThat(actual.rows()).isEqualTo(expected.rows());
    }

    private static CanonicalQueryObject query(QueryMetadata metadata, List<String> dimensions,
                                              List<String> aggregates) {
        return new CanonicalQueryObject(HashComponents.of(dimensions, aggregates, List.of()),
                new TimeRange(-86_400, 86_400), PostProcessing.none(), metadata,
                "SELECT cached partials", List.of("events"), List.of());
    }

    private static ResultFrame mixedFrame(Object[]... rows) {
        ResultFrameBuilder builder = ResultFrame.builder()
                .column("ts", ColumnType.LONG)
                .column("tenant", ColumnType.LONG)
                .column("region", ColumnType.STRING)
                .column("double_key", ColumnType.DOUBLE)
                .column("decimal_key", ColumnType.DECIMAL)
                .column("SUM(total)", ColumnType.LONG)
                .column("MAX(peak)", ColumnType.DOUBLE)
                .column("MIN(label)", ColumnType.STRING)
                .column("SUM(measure)", ColumnType.DOUBLE)
                .column("SUM(amount)", ColumnType.DECIMAL);
        for (Object[] row : rows) builder.row(row);
        return builder.build();
    }

    private static ResultFrame averageFrame(Object[]... rows) {
        ResultFrameBuilder builder = ResultFrame.builder()
                .column("device", ColumnType.STRING)
                .column(AverageIngredients.sumColumn("weighted_mean"), ColumnType.DOUBLE)
                .column(AverageIngredients.countColumn("weighted_mean"), ColumnType.LONG)
                .column(AverageIngredients.typeColumn("weighted_mean"), ColumnType.STRING)
                .column(AverageIngredients.sumColumn("weighted_amount"), ColumnType.DECIMAL)
                .column(AverageIngredients.countColumn("weighted_amount"), ColumnType.LONG)
                .column(AverageIngredients.typeColumn("weighted_amount"), ColumnType.STRING);
        for (Object[] row : rows) builder.row(row);
        return builder.build();
    }

    private static List<ResultFrame> skewedFrames() {
        Random random = new Random(0x5eedL);
        List<ResultFrame> frames = new java.util.ArrayList<>();
        for (int frame = 0; frame < 8; frame++) {
            ResultFrameBuilder builder = ResultFrame.builder().expectedRows(2_000)
                    .column("ts", ColumnType.LONG).column("device", ColumnType.LONG)
                    .column("SUM(value)", ColumnType.DOUBLE).column("SUM(events)", ColumnType.LONG);
            for (int row = 0; row < 2_000; row++) {
                long time = (long) random.nextInt(3) * 300L + random.nextInt(300);
                long device = random.nextInt(100) < 92 ? 0L : 1L + random.nextInt(2_000);
                double value = switch ((frame * 2_000 + row) % 4) {
                    case 0 -> 1.0e16;
                    case 1 -> -1.0e16;
                    case 2 -> 1.0;
                    default -> random.nextDouble();
                };
                builder.row(time, device, value, (long) random.nextInt(4));
            }
            frames.add(builder.build());
        }
        return List.copyOf(frames);
    }

    private static final class QueuedExecutor implements Executor {
        private final Queue<Runnable> queued = new ArrayDeque<>();

        @Override
        public synchronized void execute(Runnable command) {
            queued.add(command);
        }

        private void runAll() {
            while (true) {
                Runnable next;
                synchronized (this) {
                    next = queued.poll();
                }
                if (next == null) return;
                next.run();
            }
        }
    }
}
