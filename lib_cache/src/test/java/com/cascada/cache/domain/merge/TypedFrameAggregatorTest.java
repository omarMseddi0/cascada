package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TypedFrameAggregatorTest {

    @Test
    void keepsNullNanSignedZeroAndScaleInsensitiveGroupingSemantics() {
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

        assertThat(merged.rowCount()).isEqualTo(4);
        assertThat(merged.rows()).extracting(row -> row.get("SUM(x)"))
                .containsExactly(1L, 3L, 2L, 9L);
        assertThat(merged.rows().get(0).get("d")).isNull();
        assertThat(Double.doubleToLongBits((Double) merged.rows().get(1).get("d")))
                .isEqualTo(Double.doubleToLongBits(-0.0));
        assertThat(Double.doubleToLongBits((Double) merged.rows().get(2).get("d")))
                .isEqualTo(Double.doubleToLongBits(0.0));
        assertThat((Double) merged.rows().get(3).get("d")).isNaN();
        // The first row representing a numeric decimal group keeps its original scale.
        assertThat(merged.rows().get(1).get("decimal_id")).isEqualTo(new BigDecimal("1.00"));
        assertThat(merged.rows().get(3).get("decimal_id")).isEqualTo(new BigDecimal("1.0"));
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
