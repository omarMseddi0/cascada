package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the single biggest hit-rate multiplier: a coarser cached shape answers a finer query in
 * memory, and — critically — that non-subsumable shapes are rejected (a false-positive subsumption
 * would silently return a wrong number).
 */
class CubeSubsumptionPlannerTest {

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();

    /** Cached at grain (appName, deviceType): four rows. */
    private ResultFrame cachedByAppAndDevice() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(bytes)", 10.0))
                .row(Map.of("appName", "netflix", "deviceType", "tablet", "SUM(bytes)", 5.0))
                .row(Map.of("appName", "youtube", "deviceType", "mobile", "SUM(bytes)", 7.0))
                .row(Map.of("appName", "youtube", "deviceType", "tablet", "SUM(bytes)", 3.0))
                .build();
    }

    private CachedShapeEntry candidateEntry() {
        return new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(bytes)")),
                cachedByAppAndDevice());
    }

    @Test
    void rollsUpByDroppingAGroupByColumn() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));
        Optional<CachedShapeEntry> subsuming =
                planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()));
        assertThat(subsuming).isPresent();

        ResultFrame rolledUp = planner.rollUpAndFilterDown(subsuming.get(), query);
        Map<String, Double> byApp = byApp(rolledUp);
        assertThat(byApp.get("netflix")).isEqualTo(15.0); // 10 + 5
        assertThat(byApp.get("youtube")).isEqualTo(10.0); // 7 + 3
    }

    @Test
    void filtersDownByAnAddedEqualityPredicate() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of("appName = 'youtube'"), Set.of("SUM(bytes)"));
        Optional<CachedShapeEntry> subsuming =
                planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()));
        assertThat(subsuming).isPresent();

        ResultFrame answer = planner.rollUpAndFilterDown(subsuming.get(), query);
        Map<String, Double> byApp = byApp(answer);
        assertThat(byApp).containsOnlyKeys("youtube");
        assertThat(byApp.get("youtube")).isEqualTo(10.0);
    }

    @Test
    void rejectsWhenCandidateGroupByIsNotASuperset() {
        QueryShape query = new QueryShape(Set.of("appName", "region"), Set.of(), Set.of("SUM(bytes)"));
        assertThat(planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()))).isEmpty();
    }

    @Test
    void rejectsHolisticAggregatesThatCannotRollUpExactly() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("COUNT(DISTINCT subscriberId)"));
        assertThat(planner.areAggregatesCompatible(query)).isFalse();
        assertThat(planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()))).isEmpty();
    }

    @Test
    void rejectsWhenCandidateHasAFilterTheQueryDoesNotHave() {
        // candidate is already filtered to mobile; a query without that filter cannot be answered from it
        CachedShapeEntry filteredCandidate = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType"), Set.of("deviceType = 'mobile'"), Set.of("SUM(bytes)")),
                cachedByAppAndDevice());
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));
        assertThat(planner.findSubsumingCacheEntryForQuery(query, List.of(filteredCandidate))).isEmpty();
    }

    @Test
    void exactSameShapeSubsumesItself() {
        QueryShape query = new QueryShape(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(bytes)"));
        assertThat(planner.subsumes(query, query)).isTrue();
    }

    @Test
    void rolledAwayNumericDimensionIsDroppedNotSummedAsAMeasure() {
        // Candidate grouped by (appName, hourBucket); hourBucket is numeric. Rolling up to appName
        // must DROP hourBucket — summing it would fabricate a column of added epoch hours.
        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("hourBucket", ColumnType.LONG)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "hourBucket", 3_600L, "SUM(bytes)", 10.0))
                .row(Map.of("appName", "netflix", "hourBucket", 7_200L, "SUM(bytes)", 5.0))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "hourBucket"), Set.of(), Set.of("SUM(bytes)")), frame);
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));

        ResultFrame rolledUp = planner.rollUpAndFilterDown(candidate, query);

        assertThat(rolledUp.columnNames()).containsExactly("appName", "SUM(bytes)");
        assertThat(byApp(rolledUp).get("netflix")).isEqualTo(15.0);
    }

    @Test
    void inListMembersWithQuotedCommasAreParsedAsSingleMembers() {
        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "net,flix", "SUM(bytes)", 10.0))
                .row(Map.of("appName", "flix", "SUM(bytes)", 99.0))
                .row(Map.of("appName", "youtube", "SUM(bytes)", 7.0))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)")), frame);
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("appName IN ('net,flix', 'youtube')"), Set.of("SUM(bytes)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        // a naive split(",") would admit 'flix' (99.0) and drop 'net,flix'
        Map<String, Double> byApp = byApp(answer);
        assertThat(byApp).containsOnlyKeys("net,flix", "youtube");
        assertThat(byApp.get("net,flix")).isEqualTo(10.0);
    }

    @Test
    void numericEqualityFilterMatchesACellStoredAsADouble() {
        ResultFrame frame = ResultFrame.builder()
                .column("deviceId", ColumnType.DOUBLE)
                .column("appName", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("deviceId", 5.0, "appName", "netflix", "SUM(bytes)", 10.0))
                .row(Map.of("deviceId", 6.0, "appName", "netflix", "SUM(bytes)", 4.0))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceId"), Set.of(), Set.of("SUM(bytes)")), frame);
        QueryShape query = new QueryShape(Set.of("appName"), Set.of("deviceId = 5"), Set.of("SUM(bytes)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        // the 5.0 cell must match the literal 5; a string-only comparison ("5.0" vs "5") drops the row
        assertThat(byApp(answer).get("netflix")).isEqualTo(10.0);
    }

    @Test
    void inListMembersWithEscapedQuotesMatchTheLiteralQuoteCharacter() {
        ResultFrame frame = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "o'brien", "SUM(bytes)", 10.0))
                .row(Map.of("appName", "obrien", "SUM(bytes)", 99.0))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)")), frame);
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("appName IN ('o''brien')"), Set.of("SUM(bytes)"));

        Map<String, Double> byApp = byApp(planner.rollUpAndFilterDown(candidate, query));

        assertThat(byApp).containsOnlyKeys("o'brien");
        assertThat(byApp.get("o'brien")).isEqualTo(10.0);
    }

    @Test
    void rejectsAnExtraFilterOnAColumnTheCandidateDidNotGroupBy() {
        // region is not in the candidate's group-by, so the predicate cannot be applied in memory
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("region = 'eu'"), Set.of("SUM(bytes)"));
        assertThat(planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()))).isEmpty();
    }

    @Test
    void rejectsAnExtraFilterItCannotParse() {
        // a range predicate cannot be evaluated against the cached frame in memory
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("appName LIKE 'net%'"), Set.of("SUM(bytes)"));
        assertThat(planner.findSubsumingCacheEntryForQuery(query, List.of(candidateEntry()))).isEmpty();
    }

    private Map<String, Double> byApp(ResultFrame frame) {
        Map<String, Double> result = new java.util.HashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            result.put((String) row.get("appName"), ((Number) row.get("SUM(bytes)")).doubleValue());
        }
        return result;
    }
}
