package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.exception.CubeRollUpUnavailableException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CubeRollupOrderRegressionTest {

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();

    @Test
    void preservesNullCanonicalGroupsExactLongsAndFirstEncounterDimensionValues() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("id", ColumnType.LONG)
                .column("score", ColumnType.DOUBLE)
                .column("amount", ColumnType.DECIMAL)
                .column("shard", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row(null, null, null, null, "a", 9_007_199_254_740_992L)
                .row(null, null, null, null, "b", 1L)
                .row("null", Long.MAX_VALUE, -0.0d, new BigDecimal("1.0"), "a", 2L)
                .row("null", Long.MAX_VALUE, 0.0d, new BigDecimal("1.00"), "b", 3L)
                .row("other", Long.MAX_VALUE - 1L, Double.longBitsToDouble(0x7ff8000000000001L),
                        new BigDecimal("2.0"), "a", 4L)
                .row("other", Long.MAX_VALUE - 1L, Double.NaN, new BigDecimal("2.00"), "b", 5L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "id", "score", "amount", "shard"), Set.of(), Set.of("SUM(x)")),
                frame);
        QueryShape query = new QueryShape(Set.of("city", "id", "score", "amount"), Set.of(), Set.of("SUM(x)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.rowCount()).isEqualTo(3);
        assertThat(answer.rows().get(0)).containsEntry("city", null).containsEntry("id", null)
                .containsEntry("score", null).containsEntry("amount", null)
                .containsEntry("SUM(x)", 9_007_199_254_740_993L);
        assertThat(answer.rows().get(1).get("city")).isEqualTo("null");
        assertThat(answer.longAt(1, answer.columnIndex("id"))).isEqualTo(Long.MAX_VALUE);
        assertThat(Double.doubleToRawLongBits(answer.doubleAt(1, answer.columnIndex("score"))))
                .isEqualTo(Double.doubleToRawLongBits(-0.0d));
        assertThat(((BigDecimal) answer.valueAt(1, answer.columnIndex("amount"))).scale()).isEqualTo(1);
        assertThat(answer.rows().get(1).get("SUM(x)")).isEqualTo(5L);
        assertThat(answer.doubleAt(2, answer.columnIndex("score"))).isNaN();
        assertThat(answer.rows().get(2).get("SUM(x)")).isEqualTo(9L);
    }

    @Test
    void combinesMinAndMaxWithSparkNanOrderingAndKeepsProjectionOrder() {
        ResultFrame frame = ResultFrame.builder()
                .column("region", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("MIN(score)", ColumnType.DOUBLE)
                .column("peak", ColumnType.DOUBLE)
                .column("total", ColumnType.LONG)
                .row("north", "a", 2.0d, 2.0d, 10L)
                .row("north", "b", Double.NaN, Double.NaN, 5L)
                .build();
        QueryShape fine = new QueryShape(Set.of("region", "shard"), Set.of(),
                Set.of("MIN(score)", "MAX(score) AS peak", "SUM(x) AS total"), Set.of("traffic"), Map.of(),
                List.of("region", "shard", "MIN(score)", "MAX(score) AS peak", "SUM(x) AS total"));
        CachedShapeEntry candidate = new CachedShapeEntry(fine, frame);
        QueryShape coarse = new QueryShape(Set.of("region"), Set.of(),
                Set.of("MIN(score)", "MAX(score) AS peak", "SUM(x) AS total"), Set.of("traffic"), Map.of(),
                List.of("SUM(x) AS total", "region", "MIN(score)", "MAX(score) AS peak"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, coarse);

        assertThat(answer.columnNames()).containsExactly("total", "region", "MIN(score)", "peak");
        assertThat(answer.longAt(0, answer.columnIndex("total"))).isEqualTo(15L);
        assertThat(answer.doubleAt(0, answer.columnIndex("MIN(score)"))).isEqualTo(2.0d);
        assertThat(answer.doubleAt(0, answer.columnIndex("peak"))).isNaN();
    }

    @Test
    void continuesToRejectStringAggregateOutputs() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("first_city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("Paris", "a", "Paris", 1L)
                .row("Paris", "b", "Lyon", 2L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "shard"), Set.of(), Set.of("MIN(city) AS first_city", "SUM(x)")),
                frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(),
                Set.of("MIN(city) AS first_city", "SUM(x)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("string aggregate output");
    }

    @Test
    void producesNoGlobalGroupWhenEveryInputRowIsFilteredOut() {
        ResultFrame frame = ResultFrame.builder()
                .column("region", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("north", "a", 1L)
                .row("south", "b", 2L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("region", "shard"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of(), Set.of("region = 'absent'"), Set.of("SUM(x)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.rowCount()).isZero();
    }

    @Test
    void keepsUnsupportedPredicateErrorsDuringTheTypedScan() {
        ResultFrame frame = ResultFrame.builder()
                .column("region", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("north", "a", 1L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("region", "shard"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of(), Set.of("region LIKE 'n%'"), Set.of("SUM(x)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class);
    }
}
