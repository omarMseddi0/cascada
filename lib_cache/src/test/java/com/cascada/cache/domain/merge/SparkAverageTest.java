package com.cascada.cache.domain.merge;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** AVG rebuilt from merged SUM and COUNT ingredients with Spark's result type and rounding. */
class SparkAverageTest {

    private static ResultFrame ingredients(ColumnType sumType, Object sum, Long count, String type) {
        return ResultFrame.builder().column("region", ColumnType.STRING)
                .column(AverageIngredients.sumColumn("mean"), sumType)
                .column(AverageIngredients.countColumn("mean"), ColumnType.LONG)
                .column(AverageIngredients.typeColumn("mean"), ColumnType.STRING)
                .column("total", ColumnType.LONG)
                .row("a", sum, count, type, 7L).build();
    }

    @Test
    void integralAverageIsSumOverCountAsDoubleAtTheAliasPosition() {
        ResultFrame answer = SparkAverage.finish(ingredients(ColumnType.LONG, 10L, 4L, "bigint"), List.of("mean"));

        assertThat(answer.columnNames()).containsExactly("region", "mean", "total");
        assertThat(answer.columnType("mean")).isEqualTo(ColumnType.DOUBLE);
        assertThat(answer.doubleAt(0, 1)).isEqualTo(2.5);
    }

    @Test
    void decimalAverageUsesSparkResultScaleAndHalfUpRounding() {
        // AVG over decimal(18,2) is decimal(22,6): 2/3 -> 0.666667, and 1.00/8 -> 0.125000.
        ResultFrame twoThirds = SparkAverage.finish(
                ingredients(ColumnType.DECIMAL, new BigDecimal("2.00"), 3L, "decimal(18,2)"), List.of("mean"));
        assertThat(twoThirds.columnType("mean")).isEqualTo(ColumnType.DECIMAL);
        assertThat(twoThirds.valueAt(0, 1)).isEqualTo(new BigDecimal("0.666667"));

        ResultFrame eighth = SparkAverage.finish(
                ingredients(ColumnType.DECIMAL, new BigDecimal("1.00"), 8L, "decimal(18,2)"), List.of("mean"));
        assertThat(eighth.valueAt(0, 1)).isEqualTo(new BigDecimal("0.125000"));
    }

    @Test
    void zeroCountYieldsNull() {
        ResultFrame answer = SparkAverage.finish(ingredients(ColumnType.LONG, null, 0L, "bigint"), List.of("mean"));
        assertThat(answer.isNullAt(0, 1)).isTrue();
    }
}
