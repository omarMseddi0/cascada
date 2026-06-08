package com.cascada.cache.application;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
    void reconstructsAnAdditiveCompositeAliasFromStoredIngredients() {
        // The query asked for SUM(bytesFromClient) + SUM(bytesFromServer) AS total_bytes; the cache
        // stores the two SUM ingredients. Without reconstruction the result lacks total_bytes.
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total_bytes", "SUM(bytesFromClient) + SUM(bytesFromServer)"));
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
    void leavesACompositeAliasAbsentWhenAnIngredientColumnIsMissing() {
        // Defensive: if the formula references a column not in the frame, do NOT fabricate the alias.
        QueryMetadata metadata = QueryMetadata.globalAggregate()
                .withCompositeAliases(Map.of("total_bytes", "SUM(bytesFromClient) + SUM(missing)"));
        CanonicalQueryObject canonical = globalAgg(List.of("appName"),
                List.of("SUM(bytesFromClient)"), metadata);
        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytesFromClient)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "SUM(bytesFromClient)", 100.0))
                .build();

        ResultFrame result = merge.mergeAndReconstruct(List.of(frame), canonical);
        assertThat(result.columnNames()).doesNotContain("total_bytes");
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
}
