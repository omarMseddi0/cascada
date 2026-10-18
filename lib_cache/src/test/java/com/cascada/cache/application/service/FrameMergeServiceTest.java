package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.query.FormulaBinary;
import com.cascada.cache.domain.model.query.FormulaColumn;
import com.cascada.cache.domain.query.ArithmeticOperator;
import com.cascada.cache.domain.model.time.TimeRange;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the merge-output reconstruction paths that bring the Java engine to parity with the Python
 * reference (merging.py): composite-alias reconstruction (SUM(a)+SUM(b) AS total_bytes) and the
 * global-aggregate time flooring that aligns a cached bucket row with a Spark-gap row in the same
 * fixed-step window so they are not counted as two groups.
 */
class FrameMergeServiceTest {

    private final FrameMergeService merge = new FrameMergeService(300, "ts");

    private CanonicalQueryObject globalAgg(List<String> groupBy, List<String> aggregates,
                                          QueryMetadata metadata) {
        HashComponents components = HashComponents.of(groupBy, aggregates, List.of());
        return new CanonicalQueryObject(components, new TimeRange(0, 86_399), PostProcessing.none(),
                metadata, "FULL_SQL", List.of("traffic"), List.of());
    }

    @Test
    void combinesAnAliasedMaxByMaximumWhenTheParsedAggregateMapIsPresent() {
        // README caveat 4: MAX(latency) AS peak_latency stores a column whose NAME carries no max
        // signal — the old name-sniffing combiner SUMmed it across buckets. The parsed map recorded at
        // canonicalization time is authoritative.
        QueryMetadata metadata = QueryMetadata.globalAggregate().withMeasureAggregates(
                Map.of("peak_latency", com.cascada.cache.domain.merge.AggregateFunction.MAXIMUM));
        CanonicalQueryObject canonical = globalAgg(List.of("appName"),
                List.of("MAX(latency)"), metadata);

        ResultFrame monday = ResultFrame.builder()
                .column("appName", ColumnType.STRING).column("peak_latency", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "peak_latency", 9.0)).build();
        ResultFrame tuesday = ResultFrame.builder()
                .column("appName", ColumnType.STRING).column("peak_latency", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "peak_latency", 5.0)).build();

        ResultFrame result = merge.mergeAndReconstruct(List.of(monday, tuesday), canonical);

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(((Number) result.rows().get(0).get("peak_latency")).doubleValue())
                .isEqualTo(9.0); // MAX, not 14.0 (the SUM the sniffing heuristic produced)
    }

    @Test
    void combinesAnAliasedMaxByMaximumFromTheAggregateSpecsFallback() {
        // Same correctness through the aggregate-spec strings alone (older canonical objects that
        // carry specs but no explicit measure map).
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withAggregateSpecs(List.of("MAX(latency) AS peak_latency"));
        CanonicalQueryObject canonical = globalAgg(List.of("appName"),
                List.of("MAX(latency)"), metadata);

        ResultFrame monday = ResultFrame.builder()
                .column("appName", ColumnType.STRING).column("peak_latency", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "peak_latency", 3.0)).build();
        ResultFrame tuesday = ResultFrame.builder()
                .column("appName", ColumnType.STRING).column("peak_latency", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "peak_latency", 7.0)).build();

        ResultFrame result = merge.mergeAndReconstruct(List.of(monday, tuesday), canonical);
        assertThat(((Number) result.rows().get(0).get("peak_latency")).doubleValue()).isEqualTo(7.0);
    }

    @Test
    void reconstructsAnAdditiveCompositeAliasFromStoredIngredients() {
        // The query asked for SUM(bytesFromClient) + SUM(bytesFromServer) AS total_bytes; the cache
        // stores the two SUM ingredients. Without reconstruction the result lacks total_bytes.
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total_bytes", "SUM(bytesFromClient) + SUM(bytesFromServer)"))
                .withCompositeFormulas(Map.of("total_bytes", new FormulaBinary(ArithmeticOperator.ADD,
                        new FormulaColumn("SUM(bytesFromClient)"), new FormulaColumn("SUM(bytesFromServer)"))));
        CanonicalQueryObject canonical = globalAgg(List.of("appName"),
                List.of("SUM(bytesFromClient)", "SUM(bytesFromServer)"), metadata);

        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytesFromClient)", ColumnType.DOUBLE)
                .column("SUM(bytesFromServer)", ColumnType.DOUBLE)
                .row(rowOf("netflix", 100.0, 25.0))
                .build();

        ResultFrame result = merge.mergeAndReconstruct(List.of(frame), canonical);

        assertThat(result.columnNames()).contains("total_bytes");
        Map<String, Object> row = result.rows().get(0);
        assertThat(((Number) row.get("total_bytes")).doubleValue()).isEqualTo(125.0);
    }

    @Test
    void rejectsAMissingCompositeIngredientSoTheEngineCanUseTheAuthoritativeFallback() {
        // Defensive: if the formula references a column not in the frame, do NOT fabricate the alias.
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total_bytes", "SUM(bytesFromClient) + SUM(missing)"))
                .withCompositeFormulas(Map.of("total_bytes", new FormulaBinary(ArithmeticOperator.ADD,
                        new FormulaColumn("SUM(bytesFromClient)"), new FormulaColumn("SUM(missing)"))));
        CanonicalQueryObject canonical = globalAgg(List.of("appName"),
                List.of("SUM(bytesFromClient)"), metadata);
        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytesFromClient)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "SUM(bytesFromClient)", 100.0))
                .build();

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(frame), canonical))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing composite ingredient");
    }

    @Test
    void floorsTheGroupedTimeColumnSoCacheAndGapRowsInTheSameStepCollapse() {
        // Grouping by ts: a cached row at ts=1200 (already floored to the 300s bucket) and a Spark-gap
        // row at ts=1350 (same 300s window: floor(1350/300)*300 = 1200) must merge into ONE group.
        QueryMetadata metadata = QueryMetadata.globalAggregate();
        CanonicalQueryObject canonical = globalAgg(List.of("ts"), List.of("SUM(bytes)"), metadata);

        ResultFrame fromCache = ResultFrame.builder()
                .column("ts", ColumnType.LONG).column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("ts", 1200L, "SUM(bytes)", 10.0))
                .build();
        ResultFrame fromSpark = ResultFrame.builder()
                .column("ts", ColumnType.LONG).column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("ts", 1350L, "SUM(bytes)", 5.0)) // same 300s bucket as 1200
                .build();

        ResultFrame result = merge.mergeAndReconstruct(List.of(fromCache, fromSpark), canonical);

        assertThat(result.rowCount()).isEqualTo(1); // collapsed, not two separate ts groups
        Map<String, Object> row = result.rows().get(0);
        // In the global-aggregate path a grouped dimension is keyed as a string; the floored bucket is 1200.
        assertThat(String.valueOf(row.get("ts"))).isEqualTo("1200");
        assertThat(((Number) row.get("SUM(bytes)")).doubleValue()).isEqualTo(15.0);
    }

    private Map<String, Object> rowOf(String app, double client, double server) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("appName", app);
        values.put("SUM(bytesFromClient)", client);
        values.put("SUM(bytesFromServer)", server);
        return values;
    }

    @Test void preservesRawEpochSecondGroupsInBatchAndIncrementalMerges() {
        QueryMetadata raw = new QueryMetadata(true, 0L, List.of(), Map.of(),
                java.util.Optional.empty(), true, List.of("SUM(bytes)"));
        CanonicalQueryObject canonical = globalAgg(List.of("ts"), List.of("SUM(bytes)"), raw);
        ResultFrame cached = ResultFrame.builder().column("ts", ColumnType.LONG)
                .column("SUM(bytes)", ColumnType.LONG)
                .row(Map.of("ts", -301L, "SUM(bytes)", 7L))
                .row(Map.of("ts", 1759968001L, "SUM(bytes)", 11L)).build();
        ResultFrame gap = ResultFrame.builder().column("ts", ColumnType.LONG)
                .column("SUM(bytes)", ColumnType.LONG)
                .row(Map.of("ts", -300L, "SUM(bytes)", 13L))
                .row(Map.of("ts", 1759968002L, "SUM(bytes)", 17L))
                .row(Map.of("ts", 1759968001L, "SUM(bytes)", 19L)).build();
        var incremental = merge.incremental(canonical);
        incremental.add(cached);
        incremental.add(gap);
        for (ResultFrame result : List.of(merge.mergeAndReconstruct(List.of(cached, gap), canonical), incremental.finish())) {
            assertThat(result.rows()).containsExactly(
                    Map.of("ts", -301L, "SUM(bytes)", 7L),
                    Map.of("ts", -300L, "SUM(bytes)", 13L),
                    Map.of("ts", 1759968001L, "SUM(bytes)", 30L),
                    Map.of("ts", 1759968002L, "SUM(bytes)", 17L));
        }
    }

    @Test void averageWithAllNullIngredientsIsSqlNullInBothMergePaths() {
        var metadata = QueryMetadata.globalAggregate().withOriginalAggregates(List.of("AVG(x) AS mean"));
        var canonical = globalAgg(List.of(), List.of("SUM(x)", "COUNT(x)"), metadata);
        var frame = ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG)
                .row(null, 0L).build();
        var incremental = merge.incremental(canonical);
        incremental.add(frame);
        for (var result : List.of(merge.mergeAndReconstruct(List.of(frame), canonical), incremental.finish())) {
            assertThat(result.columnNames()).containsExactly("mean");
            assertThat(result.isNullAt(0, 0)).isTrue();
        }
    }

    @Test
    void averageHandlesZeroAndLargeLongCountsAndRejectsNegativeCounts() {
        var metadata = QueryMetadata.globalAggregate().withOriginalAggregates(List.of("AVG(x) AS mean"));
        var canonical = globalAgg(List.of(), List.of("SUM(x)", "COUNT(x)"), metadata);
        long count = 9_007_199_254_740_993L;
        var large = ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG)
                .row(18_014_398_509_481_984.0, count).build();
        assertThat(merge.mergeAndReconstruct(List.of(large), canonical).doubleAt(0, 0))
                .isEqualTo(18_014_398_509_481_984.0 / count);
        var zero = ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG)
                .row(0.0, 0L).build();
        assertThat(merge.mergeAndReconstruct(List.of(zero), canonical).isNullAt(0, 0)).isTrue();
        for (Object sum : java.util.Arrays.asList(null, 1.0)) {
            var negative = ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG)
                    .row(sum, -1L).build();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> merge.mergeAndReconstruct(List.of(negative), canonical))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("negative");
        }
    }
}
