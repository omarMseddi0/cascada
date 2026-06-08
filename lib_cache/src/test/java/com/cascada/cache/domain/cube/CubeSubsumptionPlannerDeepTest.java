package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Aggressive coverage of the Appendix-I cube algebra enhancements: best-subsumer selection (I.4),
 * AVG reconstruction in roll-up (I.2/2), IN-list filter-down, and the I.2/3 grouped-column guard that
 * stops a predicate on a non-grouped column from being (wrongly) treated as narrowable.
 */
class CubeSubsumptionPlannerDeepTest {

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();

    /** Cached at the finest grain (appName, deviceType, region) with SUM+COUNT for AVG reconstruction. */
    private ResultFrame finestFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .column("COUNT(latency)", ColumnType.DOUBLE)
                .row(row("netflix", "mobile", "eu", 100.0, 4.0))
                .row(row("netflix", "tablet", "eu", 50.0, 1.0))
                .row(row("netflix", "mobile", "us", 30.0, 2.0))
                .row(row("youtube", "mobile", "eu", 20.0, 2.0))
                .build();
    }

    private Map<String, Object> row(String app, String device, String region, double sum, double count) {
        Map<String, Object> values = new HashMap<>();
        values.put("appName", app);
        values.put("deviceType", device);
        values.put("region", region);
        values.put("SUM(latency)", sum);
        values.put("COUNT(latency)", count);
        return values;
    }

    private CachedShapeEntry finest() {
        return new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType", "region"), Set.of(),
                        Set.of("SUM(latency)", "COUNT(latency)")),
                finestFrame());
    }

    @Test
    void reconstructsAverageDuringRollUp() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("AVG(latency)"));
        assertThat(planner.subsumes(finest().shape(), query)).isTrue();

        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);
        Map<String, Double> avgByApp = new HashMap<>();
        for (Map<String, Object> r : answer.rows()) {
            avgByApp.put((String) r.get("appName"), ((Number) r.get("AVG(latency)")).doubleValue());
        }
        // netflix: (100+50+30)/(4+1+2) = 180/7; youtube: 20/2 = 10
        assertThat(avgByApp.get("netflix")).isEqualTo(180.0 / 7.0);
        assertThat(avgByApp.get("youtube")).isEqualTo(10.0);
    }

    @Test
    void averageReconstructionEqualsDirectComputeAcrossTheWholeCube() {
        // Independent ground truth: AVG over all rows = total sum / total count.
        QueryShape query = new QueryShape(Set.of(), Set.of(), Set.of("AVG(latency)"));
        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);
        double avg = ((Number) answer.rows().get(0).get("AVG(latency)")).doubleValue();
        assertThat(avg).isEqualTo((100.0 + 50.0 + 30.0 + 20.0) / (4.0 + 1.0 + 2.0 + 2.0));
    }

    @Test
    void filtersDownByAnInList() {
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("region IN ('eu', 'us')"), Set.of("SUM(latency)"));
        assertThat(planner.subsumes(finest().shape(), query)).isTrue();

        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);
        Map<String, Double> sumByApp = new HashMap<>();
        for (Map<String, Object> r : answer.rows()) {
            sumByApp.put((String) r.get("appName"), ((Number) r.get("SUM(latency)")).doubleValue());
        }
        assertThat(sumByApp.get("netflix")).isEqualTo(180.0); // all three netflix rows are eu/us
        assertThat(sumByApp.get("youtube")).isEqualTo(20.0);
    }

    @Test
    void rejectsAvgWhenCandidateLacksSumOrCount() {
        CachedShapeEntry sumOnly = new CachedShapeEntry(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)")),
                ResultFrame.builder().column("appName", ColumnType.STRING)
                        .column("SUM(latency)", ColumnType.DOUBLE)
                        .row(Map.of("appName", "netflix", "SUM(latency)", 1.0)).build());
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("AVG(latency)"));
        assertThat(planner.subsumes(sumOnly.shape(), query)).isFalse();
    }

    @Test
    void rejectsAFilterOnANonGroupedColumn() {
        // candidate groups by appName only; a predicate on deviceType cannot be applied in memory.
        CachedShapeEntry byApp = new CachedShapeEntry(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)")),
                ResultFrame.builder().column("appName", ColumnType.STRING)
                        .column("SUM(latency)", ColumnType.DOUBLE)
                        .row(Map.of("appName", "netflix", "SUM(latency)", 1.0)).build());
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("deviceType = 'mobile'"), Set.of("SUM(latency)"));
        assertThat(planner.subsumes(byApp.shape(), query)).isFalse();
    }

    @Test
    void bestSubsumerPrefersTheSmallestGroupBySuperset() {
        CachedShapeEntry coarser = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(latency)", "COUNT(latency)")),
                finestFrame()); // frame content irrelevant for the selection assertion
        CachedShapeEntry finer = finest(); // 3 group-by cols

        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)"));
        Optional<CachedShapeEntry> best =
                planner.findBestSubsumingCacheEntryForQuery(query, List.of(finer, coarser));
        assertThat(best).isPresent();
        assertThat(best.get().shape().groupBy()).hasSize(2); // the coarser (cheaper) one
    }

    @Test
    void bestSubsumerTieBreaksOnTightestFilters() {
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("region = 'eu'"), Set.of("SUM(latency)"));
        CachedShapeEntry unfiltered = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "region"), Set.of(), Set.of("SUM(latency)")), finestFrame());
        CachedShapeEntry preFiltered = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "region"), Set.of("region = 'eu'"), Set.of("SUM(latency)")),
                finestFrame());

        Optional<CachedShapeEntry> best =
                planner.findBestSubsumingCacheEntryForQuery(query, List.of(unfiltered, preFiltered));
        assertThat(best).isPresent();
        assertThat(best.get().shape().filters()).contains("region = 'eu'");
    }
}
