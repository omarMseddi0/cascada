package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void preservesSpacesInsideQuotedEqualityAndInListLiterals() {
        ResultFrame frame = ResultFrame.builder()
                .column("region", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("north", 10L)
                .row(" north ", 20L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("region"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape inFilter = new QueryShape(Set.of(),
                Set.of("region IN (' north ')"), Set.of("SUM(x)"));
        QueryShape equalityFilter = new QueryShape(Set.of(),
                Set.of("region = ' north '"), Set.of("SUM(x)"));

        ResultFrame inAnswer = planner.rollUpAndFilterDown(candidate, inFilter);
        ResultFrame equalityAnswer = planner.rollUpAndFilterDown(candidate, equalityFilter);

        assertThat(inAnswer.rows()).containsExactly(Map.of("SUM(x)", 20L));
        assertThat(equalityAnswer.rows()).containsExactly(Map.of("SUM(x)", 20L));
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

    @Test
    void keepsNullAndTheLiteralNullInSeparateGroups() {
        Map<String, Object> nullCity = new java.util.HashMap<>();
        nullCity.put("city", null);
        nullCity.put("device", "phone");
        nullCity.put("SUM(x)", 10.0);
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("device", ColumnType.STRING)
                .column("SUM(x)", ColumnType.DOUBLE)
                .row(nullCity)
                .row(Map.of("city", "null", "device", "tablet", "SUM(x)", 20.0))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "device"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.columnType("city")).isEqualTo(ColumnType.STRING);
        assertThat(answer.rowCount()).isEqualTo(2);
        assertThat(answer.rows()).anySatisfy(row -> {
            assertThat(row.get("city")).isNull();
            assertThat(row.get("SUM(x)")).isEqualTo(10.0);
        });
        assertThat(answer.rows()).anySatisfy(row -> {
            assertThat(row.get("city")).isEqualTo("null");
            assertThat(row.get("SUM(x)")).isEqualTo(20.0);
        });
    }

    @Test
    void preservesLongDimensionAndExactLongMeasureAboveDoublePrecision() {
        ResultFrame frame = ResultFrame.builder()
                .column("id", ColumnType.LONG)
                .column("shard", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row(5L, "shard-a", 9_007_199_254_740_992L)
                .row(5L, "shard-b", 1L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("id", "shard"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("id"), Set.of(), Set.of("SUM(x)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.columnType("id")).isEqualTo(ColumnType.LONG);
        assertThat(answer.columnType("SUM(x)")).isEqualTo(ColumnType.LONG);
        assertThat(answer.rows().get(0).get("id")).isEqualTo(5L);
        assertThat(answer.rows().get(0).get("SUM(x)")).isEqualTo(9_007_199_254_740_993L);
    }

    @Test
    void rollsUpHighPrecisionDecimalWithoutConvertingThroughDouble() {
        BigDecimal first = new BigDecimal("90071992547409931234567890.1234");
        BigDecimal second = new BigDecimal("0.0001");
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("SUM(amount)", ColumnType.DECIMAL)
                .row("Paris", "shard-a", first)
                .row("Paris", "shard-b", second)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "shard"), Set.of(), Set.of("SUM(amount)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(amount)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.columnType("SUM(amount)")).isEqualTo(ColumnType.DECIMAL);
        assertThat((BigDecimal) answer.rows().get(0).get("SUM(amount)"))
                .isEqualByComparingTo("90071992547409931234567890.1235");
    }

    @Test
    void refusesDecimalRollUpThatWouldExceedSparkPrecision() {
        BigDecimal nearLimit = new BigDecimal("9".repeat(38));
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("shard", ColumnType.STRING)
                .column("SUM(amount)", ColumnType.DECIMAL)
                .row("Paris", "shard-a", nearLimit)
                .row("Paris", "shard-b", nearLimit)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "shard"), Set.of(), Set.of("SUM(amount)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(amount)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("precision/scale");
    }

    @Test
    void refusesDecimalValuesWithMoreThan38IntegerDigitsAtNegativeScale() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("SUM(amount)", ColumnType.DECIMAL)
                .row("Paris", new BigDecimal("1E+39"))
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(amount)")), frame);

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, candidate.shape()))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("precision/scale");
    }

    @Test
    void comparesLongFilterLiteralsExactlyAboveDoublePrecision() {
        ResultFrame frame = ResultFrame.builder()
                .column("id", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.DOUBLE)
                .row(9_007_199_254_740_993L, "wrong", 10.0)
                .row(9_007_199_254_740_992L, "right", 20.0)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("id", "city"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of("id = 9007199254740992"), Set.of("SUM(x)"));

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, query);

        assertThat(answer.rowCount()).isEqualTo(1);
        assertThat(answer.rows().get(0).get("city")).isEqualTo("right");
    }

    @Test
    void treatsEqualityWithSqlNullAsUnknown() {
        Map<String, Object> row = new java.util.HashMap<>();
        row.put("id", null);
        row.put("city", "Paris");
        row.put("SUM(x)", 10.0);
        ResultFrame frame = ResultFrame.builder()
                .column("id", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.DOUBLE)
                .row(row)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("id", "city"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of("id = NULL"), Set.of("SUM(x)"));

        assertThat(planner.rollUpAndFilterDown(candidate, query).rowCount()).isZero();
    }

    @Test
    void refusesQuotedNumericLiteralInsteadOfCoercingItToAnInteger() {
        ResultFrame frame = ResultFrame.builder()
                .column("id", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.DOUBLE)
                .row(5L, "Paris", 10.0)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("id", "city"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of("id = '5'"), Set.of("SUM(x)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("quoted literal");
    }

    @Test
    void keepsTheRequestedProjectionOrderAndRefusesReorderedShapeReuse() {
        List<String> requestedOrder = List.of("SUM(x) AS total", "city");
        ResultFrame frame = ResultFrame.builder()
                .column("total", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .row(15L, "Paris")
                .build();
        QueryShape fine = new QueryShape(Set.of("city", "region"), Set.of(), Set.of("SUM(x) AS total"),
                Set.of("traffic"), Map.of(), requestedOrder);
        CachedShapeEntry candidate = new CachedShapeEntry(fine, frame);
        QueryShape coarse = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x) AS total"),
                Set.of("traffic"), Map.of(), requestedOrder);

        ResultFrame answer = planner.rollUpAndFilterDown(candidate, coarse);

        assertThat(answer.columnNames()).containsExactly("total", "city");
        assertThat(answer.columnType("total")).isEqualTo(ColumnType.LONG);

        QueryShape reordered = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x) AS total"),
                Set.of("traffic"), Map.of(), List.of("city", "SUM(x) AS total"));
        assertThat(planner.subsumes(fine, reordered)).isFalse();
    }

    @Test
    void rejectsAnUnmappedNumericAliasInsteadOfDefaultingToSum() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("total", ColumnType.LONG)
                .row("Paris", "north", 10L)
                .row("Paris", "south", 20L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "region"), Set.of(), Set.of("SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("no declared mergeable aggregate function");
    }

    @Test
    void refusesConflictingAggregateMappingsForOneOutputAlias() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("total", ColumnType.LONG)
                .row("Paris", "north", 10L)
                .row("Paris", "south", 20L)
                .build();
        QueryShape candidateShape = new QueryShape(Set.of("city", "region"), Set.of(),
                Set.of("MAX(x) AS total", "SUM(x) AS total"));
        CachedShapeEntry candidate = new CachedShapeEntry(candidateShape, frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x) AS total"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("conflicting aggregate functions");
    }

    @Test
    void refusesStringAggregateOutputsInsteadOfDroppingThem() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("first_city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("Paris", "north", "Amsterdam", 10L)
                .row("Paris", "south", "Berlin", 20L)
                .build();
        CachedShapeEntry candidate = new CachedShapeEntry(
                new QueryShape(Set.of("city", "region"), Set.of(),
                        Set.of("MIN(city) AS first_city", "SUM(x)")), frame);
        QueryShape query = new QueryShape(Set.of("city"), Set.of(),
                Set.of("MIN(city) AS first_city", "SUM(x)"));

        assertThatThrownBy(() -> planner.rollUpAndFilterDown(candidate, query))
                .isInstanceOf(CubeRollUpUnavailableException.class)
                .hasMessageContaining("string aggregate output");
    }

    private Map<String, Double> byApp(ResultFrame frame) {
        Map<String, Double> result = new java.util.HashMap<>();
        for (Map<String, Object> row : frame.rows()) {
            result.put((String) row.get("appName"), ((Number) row.get("SUM(bytes)")).doubleValue());
        }
        return result;
    }
}
