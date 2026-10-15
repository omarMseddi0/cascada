package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CubeFilterRegressionTest {
    private final CubeFilterEvaluator filters = new CubeFilterEvaluator();
    private final QueryShape candidate = new QueryShape(Set.of("region"), Set.of(), Set.of("SUM(x)"));

    @Test
    void disjunctionsAreNotMisreadAsOneStringLiteral() {
        var query = new QueryShape(Set.of("region"), Set.of("region = 'a' OR region = 'b'"), Set.of("SUM(x)"));
        assertThat(filters.canNarrow(candidate, query)).isFalse();
        var catalog = new CubeShapeCatalog();
        var window = new com.cascada.cache.domain.time.TimeRange(0, 86399);
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
}
