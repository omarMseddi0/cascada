package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.FormulaBinary;
import com.cascada.cache.domain.model.query.FormulaColumn;
import com.cascada.cache.domain.model.query.OrderByClause;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.query.ArithmeticOperator;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FrameFinishFusionTest {

    private final FrameMergeService merge = new FrameMergeService(300, "ts");

    @Test
    void reconstructsCompositeSortsByHiddenColumnAndProjectsInRequestedOrder() {
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total", "SUM(a) + SUM(b)"))
                .withCompositeFormulas(Map.of("total", new FormulaBinary(ArithmeticOperator.ADD,
                        new FormulaColumn("SUM(a)"), new FormulaColumn("SUM(b)"))));
        CanonicalQueryObject canonical = query(List.of("id", "label"),
                List.of("SUM(a)", "SUM(b)", "SUM(rank)"), metadata,
                new PostProcessing(Optional.of(2), List.of(OrderByClause.forColumn("SUM(rank)", false, false))),
                List.of("total", "label"));
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG).column("label", ColumnType.STRING)
                .column("SUM(a)", ColumnType.DOUBLE).column("SUM(b)", ColumnType.DOUBLE)
                .column("SUM(rank)", ColumnType.LONG)
                .row(3L, "gamma", 8.0, 2.0, 90L)
                .row(1L, "alpha", 1.25, 0.75, 90L)
                .row(2L, "beta", 100.0, 50.0, 110L)
                .build();

        ResultFrame answer = merge.mergeAndReconstruct(List.of(input), canonical);

        assertThat(answer.columnNames()).containsExactly("total", "label");
        assertThat(answer.columnTypeAt(0)).isEqualTo(ColumnType.DOUBLE);
        assertThat(answer.columnTypeAt(1)).isEqualTo(ColumnType.STRING);
        assertThat(answer.rows()).containsExactly(
                Map.of("total", 150.0, "label", "beta"),
                Map.of("total", 2.0, "label", "alpha"));

        CanonicalQueryObject zeroLimit = query(List.of("id", "label"),
                List.of("SUM(a)", "SUM(b)", "SUM(rank)"), metadata,
                new PostProcessing(Optional.of(0), List.of(OrderByClause.forColumn("SUM(rank)", false, false))),
                List.of("total", "label"));
        ResultFrame empty = merge.mergeAndReconstruct(List.of(input), zeroLimit);
        assertThat(empty.columnNames()).containsExactly("total", "label");
        assertThat(empty.columnTypeAt(0)).isEqualTo(ColumnType.DOUBLE);
        assertThat(empty.rowCount()).isZero();
    }

    @Test
    void decimalOrderingKeepsStableTiesAndPlacesNullsLastBeforeProjection() {
        CanonicalQueryObject canonical = query(List.of("id", "amount", "label"), List.of(),
                QueryMetadata.globalAggregate(),
                new PostProcessing(Optional.of(4), List.of(OrderByClause.forColumn("amount", true, false))),
                List.of("label", "id"));
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("amount", ColumnType.DECIMAL).column("label", ColumnType.STRING)
                .row(5L, new BigDecimal("1.000"), "five")
                .row(3L, null, "three")
                .row(2L, new BigDecimal("1.0"), "two")
                .row(4L, new BigDecimal("-0.25"), "four")
                .row(1L, new BigDecimal("1.00"), "one")
                .build();

        ResultFrame answer = merge.mergeAndReconstruct(List.of(input), canonical);

        assertThat(answer.columnNames()).containsExactly("label", "id");
        assertThat(answer.rows()).containsExactly(
                Map.of("label", "four", "id", 4L),
                Map.of("label", "one", "id", 1L),
                Map.of("label", "two", "id", 2L),
                Map.of("label", "five", "id", 5L));
    }

    @Test
    void limitWithoutOrderByKeepsGroupOrderAndProjectsWithoutChangingTypes() {
        CanonicalQueryObject canonical = query(List.of("id"), List.of("SUM(value)"),
                QueryMetadata.globalAggregate(), new PostProcessing(Optional.of(2), List.of()),
                List.of("sum(value)", "ID"));
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(value)", ColumnType.DOUBLE)
                .row(3L, 30.0).row(1L, 10.0).row(2L, 20.0).build();

        ResultFrame answer = merge.mergeAndReconstruct(List.of(input), canonical);

        assertThat(answer.columnNames()).containsExactly("sum(value)", "ID");
        assertThat(answer.columnTypeAt(0)).isEqualTo(ColumnType.DOUBLE);
        assertThat(answer.columnTypeAt(1)).isEqualTo(ColumnType.LONG);
        assertThat(answer.rows()).containsExactly(
                Map.of("sum(value)", 10.0, "ID", 1L),
                Map.of("sum(value)", 20.0, "ID", 2L));
    }

    @Test
    void emptyFrameSkipsOrderValidationButStillAppliesProjection() {
        CanonicalQueryObject canonical = query(List.of("id"), List.of("SUM(value)"),
                QueryMetadata.globalAggregate(),
                new PostProcessing(Optional.of(1), List.of(OrderByClause.forColumn("missing", true, false))),
                List.of("SUM(value)"));
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(value)", ColumnType.DOUBLE).build();

        ResultFrame answer = merge.mergeAndReconstruct(List.of(input), canonical);

        assertThat(answer.columnNames()).containsExactly("SUM(value)");
        assertThat(answer.columnTypeAt(0)).isEqualTo(ColumnType.DOUBLE);
        assertThat(answer.rowCount()).isZero();
    }

    @Test
    void missingColumnsAndCompositeErrorsStillRejectTheCachedAnswer() {
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(value)", ColumnType.DOUBLE).row(1L, 10.0).build();
        CanonicalQueryObject missingOrder = query(List.of("id"), List.of("SUM(value)"),
                QueryMetadata.globalAggregate(),
                new PostProcessing(Optional.of(1), List.of(OrderByClause.forColumn("missing", true, false))),
                List.of("id"));
        assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(input), missingOrder))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ORDER BY column is absent");

        CanonicalQueryObject missingOrderAndOutput = query(List.of("id"), List.of("SUM(value)"),
                QueryMetadata.globalAggregate(),
                new PostProcessing(Optional.of(1), List.of(OrderByClause.forColumn("missing", true, false))),
                List.of("also-missing"));
        assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(input), missingOrderAndOutput))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ORDER BY column is absent");

        CanonicalQueryObject missingOutput = query(List.of("id"), List.of("SUM(value)"),
                QueryMetadata.globalAggregate(), PostProcessing.none(), List.of("not-present"));
        assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(input), missingOutput))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing output column");

        QueryMetadata composite = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total", "SUM(a) + SUM(b)"))
                .withCompositeFormulas(Map.of("total", new FormulaBinary(ArithmeticOperator.ADD,
                        new FormulaColumn("SUM(a)"), new FormulaColumn("SUM(b)"))));
        CanonicalQueryObject overflow = query(List.of("id"), List.of("SUM(a)", "SUM(b)", "SUM(rank)"), composite,
                new PostProcessing(Optional.of(1), List.of(OrderByClause.forColumn("SUM(rank)", false, false))),
                List.of("id"));
        ResultFrame overflowInput = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(a)", ColumnType.LONG).column("SUM(b)", ColumnType.LONG)
                .column("SUM(rank)", ColumnType.LONG)
                .row(1L, 1L, 0L, 100L)
                .row(2L, Long.MAX_VALUE, 1L, 0L)
                .build();
        assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(overflowInput), overflow))
                .isInstanceOf(ArithmeticException.class);
    }

    private CanonicalQueryObject query(List<String> groupBy, List<String> aggregates, QueryMetadata metadata,
                                       PostProcessing processing, List<String> outputColumns) {
        return new CanonicalQueryObject(HashComponents.of(groupBy, aggregates, List.of()),
                new TimeRange(0, 86_399), processing, metadata, "SQL", List.of(), List.of(), List.of(), outputColumns);
    }

    @Test
    void emptyFrameKeepsLegacyProjectionWithoutInspectingDeferredClauses() {
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(v)", ColumnType.LONG).build();
        CanonicalQueryObject canonical = query(List.of("id"), List.of("SUM(v)"),
                QueryMetadata.globalAggregate(), null, List.of("SUM(v)"));
        ResultFrame output = merge.mergeAndReconstruct(List.of(input), canonical);
        assertThat(output.isEmpty()).isTrue();
        assertThat(output.columnNames()).containsExactly("SUM(v)");
    }
}
