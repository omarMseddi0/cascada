package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
