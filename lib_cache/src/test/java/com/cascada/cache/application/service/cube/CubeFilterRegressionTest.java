package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CubeRollUpUnavailableException;
import java.math.BigDecimal;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.assertThat;

class CubeFilterRegressionTest {
    private final CubeFilterEvaluator filters = new CubeFilterEvaluator();
    private final QueryShape candidate = new QueryShape(Set.of("region"), Set.of(), Set.of("SUM(x)"));

    @Test
    void disjunctionsAreNotMisreadAsOneStringLiteral() {
        var query = new QueryShape(Set.of("region"), Set.of("region = 'a' OR region = 'b'"), Set.of("SUM(x)"));
        assertThat(filters.canNarrow(candidate, query)).isFalse();
        var catalog = new CubeShapeCatalog();
        var window = new com.cascada.cache.domain.model.time.TimeRange(0, 86399);
        catalog.register(window, candidate, ResultFrame.builder().column("region", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG).row("a", 1L).row("b", 2L).row("c", 3L).build());
        assertThat(catalog.tryAnswer(window, query)).isEmpty();
    }

    @Test
    void quotedStringsCanContainOperatorsAndEscapedQuotes() {
        var frame = ResultFrame.builder().column("region", ColumnType.STRING).row("a' OR b").build();
        Set<String> predicate = Set.of("region = 'a'' OR b'");
        assertThat(filters.canNarrow(candidate,
                new QueryShape(Set.of("region"), predicate, Set.of("SUM(x)")))).isTrue();
        assertThat(filters.matchesAll(frame, Map.of("region", "a' OR b"), predicate)).isTrue();
    }

    @Test
    void disjunctionAfterAnInListAlsoBypassesCubeFiltering() {
        var query = new QueryShape(Set.of("region"), Set.of("region IN ('a') OR region IN ('b')"), Set.of("SUM(x)"));
        assertThat(filters.canNarrow(candidate, query)).isFalse();
    }

    @Test
    void compiledPredicatesMatchTheRowMapPathForTypedValuesAndSqlLiterals() {
        var frame = ResultFrame.builder()
                .column("region", ColumnType.STRING)
                .column("id", ColumnType.LONG)
                .column("score", ColumnType.DOUBLE)
                .column("price", ColumnType.DECIMAL)
                .row("a' OR b", 9_007_199_254_740_993L, Double.NaN, new BigDecimal("9007199254740993.00"))
                .row("", 0L, Double.POSITIVE_INFINITY, new BigDecimal("-2.50"))
                .row(null, Long.MIN_VALUE, Double.NEGATIVE_INFINITY, null)
                .build();

        assertCompiledAgrees(frame, Set.of("region = 'a'' OR b'"), true, false, false);
        assertCompiledAgrees(frame, Set.of("region IN ('', 'a'' OR b', 'comma, value')"), true, true, false);
        assertCompiledAgrees(frame, Set.of("region IN (NULL, 'a'' OR b')"), true, false, false);
        assertCompiledAgrees(frame, Set.of("region = NULL"), false, false, false);
        assertCompiledAgrees(frame, Set.of("id = 9007199254740993"), true, false, false);
        assertCompiledAgrees(frame, Set.of("id IN (9007199254740993, 0)"), true, true, false);
        assertCompiledAgrees(frame, Set.of("score = NaN"), true, false, false);
        assertCompiledAgrees(frame, Set.of("score IN (Infinity, -Infinity, NaN)"), true, true, true);
        assertCompiledAgrees(frame, Set.of("price = 9007199254740993"), true, false, false);
        assertCompiledAgrees(frame, Set.of("price IN (9.007199254740993e15, -2.50)"), true, true, false);
    }

    @Test
    void compiledPredicatesPreserveShortCircuitAndEmptyInputParsing() {
        var frame = ResultFrame.builder().column("region", ColumnType.STRING).row("x").build();
        Set<String> shortCircuited = new LinkedHashSet<>(
                java.util.List.of("region = 'not x'", "unsupported predicate"));
        var compiled = filters.compile(shortCircuited);
        assertThat(filters.matchesAll(frame, 0, compiled)).isFalse();
        assertThat(filters.matchesAll(frame, frame.rows().get(0), shortCircuited)).isFalse();

        var empty = ResultFrame.builder().column("region", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG).build();
        var entry = new CachedShapeEntry(candidate, empty);
        var query = new QueryShape(Set.of("region"), Set.of("unsupported predicate"), Set.of("SUM(x)"));
        assertThat(new CubeSubsumptionPlanner().rollUpAndFilterDown(entry, query).rowCount()).isZero();
    }

    @Test
    void compiledPredicatesPreserveUnsupportedPredicateErrors() {
        var frame = ResultFrame.builder().column("region", ColumnType.STRING).column("id", ColumnType.LONG)
                .row("x", 9_007_199_254_740_993L).build();

        assertCompiledErrorMatches(frame, 0, Set.of("missing = 'x'"));
        assertCompiledErrorMatches(frame, 0, Set.of("region IN ('unterminated)"));
        assertCompiledErrorMatches(frame, 0, Set.of("id = '9007199254740993'"));
    }

    private void assertCompiledAgrees(ResultFrame frame, Set<String> predicate, boolean... expected) {
        var compiled = filters.compile(predicate);
        assertThat(expected).hasSize(frame.rowCount());
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            boolean legacy = filters.matchesAll(frame, frame.rows().get(rowIndex), predicate);
            boolean typed = filters.matchesAll(frame, rowIndex, compiled);
            assertThat(typed).as(predicate + " at row " + rowIndex).isEqualTo(legacy).isEqualTo(expected[rowIndex]);
        }
    }

    private void assertCompiledErrorMatches(ResultFrame frame, int rowIndex, Set<String> predicate) {
        Throwable legacy = catchThrowable(() -> filters.matchesAll(frame, frame.rows().get(rowIndex), predicate));
        Throwable typed = catchThrowable(() -> filters.matchesAll(frame, rowIndex, filters.compile(predicate)));
        assertThat(legacy).isInstanceOf(CubeRollUpUnavailableException.class);
        assertThat(typed).isInstanceOf(legacy.getClass()).hasMessage(legacy.getMessage());
    }
}
