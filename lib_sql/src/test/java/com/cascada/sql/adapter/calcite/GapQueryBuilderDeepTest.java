package com.cascada.sql.rewrite;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhaustive coverage of {@link GapQueryBuilder}, the Calcite heir to {@code sql_rewriter.py}: the
 * {@code _merge_consecutive_gaps} coalescing in every head/body/tail combination, and the
 * structure-aware {@code _redefine_spark_query_for_gaps} injection across flat SQL, mixed predicates,
 * already-stripped WHEREs, and UNION/subquery wrappers — plus the oracle-readable predicate shape and
 * {@code strip_time_conditions}.
 */
class GapQueryBuilderDeepTest {

    private static final long DAY = 86_400L;
    private final GapQueryBuilder builder = new GapQueryBuilder("ts", DAY);
    private static final Pattern TS_RANGE =
            Pattern.compile("ts\\s*>=\\s*(\\d+)\\s+AND\\s+ts\\s*<=\\s*(\\d+)");

    private GapPlan body(Long... days) {
        return new GapPlan(Optional.empty(), List.of(days), Optional.empty());
    }

    // --- merge coalescing ------------------------------------------------------------------------

    @Test
    void mergesThreeConsecutiveDaysIntoOneRange() {
        List<TimeRange> merged = builder.mergeConsecutiveGaps(body(0L, DAY, 2 * DAY));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).startTimestampSeconds()).isZero();
        assertThat(merged.get(0).endTimestampSeconds()).isEqualTo(3 * DAY - 1);
    }

    @Test
    void keepsTwoNonAdjacentDaysSeparate() {
        assertThat(builder.mergeConsecutiveGaps(body(0L, 5 * DAY))).hasSize(2);
    }

    @Test
    void sortsUnorderedBodyDaysBeforeMerging() {
        List<TimeRange> merged = builder.mergeConsecutiveGaps(body(2 * DAY, 0L, DAY));
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).startTimestampSeconds()).isZero();
    }

    @Test
    void mergesHeadBodyAndTailWhenAdjacent() {
        GapPlan plan = new GapPlan(
                Optional.of(new TimeRange(50, DAY - 1)),
                List.of(DAY),
                Optional.of(new TimeRange(2 * DAY, 2 * DAY + 100)));
        List<TimeRange> merged = builder.mergeConsecutiveGaps(plan);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).startTimestampSeconds()).isEqualTo(50);
        assertThat(merged.get(0).endTimestampSeconds()).isEqualTo(2 * DAY + 100);
    }

    @Test
    void headOnlyProducesASingleRange() {
        GapPlan plan = new GapPlan(Optional.of(new TimeRange(10, 20)), List.of(), Optional.empty());
        assertThat(builder.mergeConsecutiveGaps(plan)).containsExactly(new TimeRange(10, 20));
    }

    @Test
    void tailOnlyProducesASingleRange() {
        GapPlan plan = new GapPlan(Optional.empty(), List.of(), Optional.of(new TimeRange(30, 40)));
        assertThat(builder.mergeConsecutiveGaps(plan)).containsExactly(new TimeRange(30, 40));
    }

    @Test
    void emptyPlanMergesToNothing() {
        assertThat(builder.mergeConsecutiveGaps(body())).isEmpty();
    }

    // --- flat gap injection ----------------------------------------------------------------------

    @Test
    void replacesAWideTimeOnlyWhereWithTheGapRange() {
        String gapSql = builder.buildGapQuery(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 999999 GROUP BY appName",
                body(0L));
        assertThat(gapSql).contains("ts >= 0");
        assertThat(gapSql).contains("ts <= " + (DAY - 1));
        assertThat(gapSql).doesNotContain("999999");
    }

    @Test
    void preservesNonTimePredicatesAndAndsTheGap() {
        String gapSql = builder.buildGapQuery(
                "SELECT appName, SUM(bytes) FROM traffic "
                        + "WHERE region = 'eu' AND ts >= 0 AND ts <= 999999 GROUP BY appName",
                body(0L));
        assertThat(gapSql).contains("region = 'eu'");
        assertThat(gapSql).contains("ts <= " + (DAY - 1));
        assertThat(gapSql).doesNotContain("999999");
    }

    @Test
    void addsAWhereWhenThePhysicalSqlHasNone() {
        String gapSql = builder.buildGapQuery(
                "SELECT appName, SUM(bytes) FROM traffic GROUP BY appName", body(0L));
        assertThat(gapSql).contains("WHERE");
        assertThat(gapSql).contains("ts >= 0");
    }

    @Test
    void dropsOrderByAndLimitFromTheGapFetch() {
        String gapSql = builder.buildGapQuery(
                "SELECT appName, SUM(bytes) AS b FROM traffic WHERE ts >= 0 AND ts <= 999999 "
                        + "GROUP BY appName ORDER BY b DESC LIMIT 10",
                body(0L));
        assertThat(gapSql).doesNotContainIgnoringCase("ORDER BY");
        assertThat(gapSql).doesNotContainIgnoringCase("LIMIT");
    }

    @Test
    void emittedPredicateIsReadableByTheCorrectnessOracle() {
        String gapSql = builder.buildGapQuery(
                "SELECT SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 999999",
                body(0L, DAY));
        Matcher matcher = TS_RANGE.matcher(gapSql);
        assertThat(matcher.find()).isTrue();
        assertThat(Long.parseLong(matcher.group(1))).isZero();
    }

    @Test
    void twoSeparateGapsBecomeTwoOredRanges() {
        String gapSql = builder.buildGapQuery(
                "SELECT SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 999999",
                body(0L, 5 * DAY));
        Matcher matcher = TS_RANGE.matcher(gapSql);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        assertThat(count).isEqualTo(2);
        assertThat(gapSql).containsIgnoringCase("OR");
    }

    // --- union / subquery injection --------------------------------------------------------------

    @Test
    void injectsTheGapIntoTheInnerWhereOfASubqueryWrapper() {
        String gapSql = builder.buildGapQuery(
                "SELECT sub.bucket, SUM(sub.b) FROM "
                        + "(SELECT ts AS bucket, bytes AS b FROM traffic WHERE ts >= 0 AND ts <= 999999) AS sub "
                        + "GROUP BY sub.bucket",
                body(0L));
        assertThat(gapSql).contains("ts >= 0");
        assertThat(gapSql).contains("ts <= " + (DAY - 1));
        assertThat(gapSql).doesNotContain("999999");
    }

    @Test
    void injectsTheGapIntoEveryBranchOfAUnion() {
        String gapSql = builder.buildGapQuery(
                "SELECT ts, bytes FROM traffic WHERE ts >= 0 AND ts <= 999999 "
                        + "UNION ALL SELECT ts, bytes FROM traffic_archive WHERE ts >= 0 AND ts <= 999999",
                body(0L));
        Matcher matcher = TS_RANGE.matcher(gapSql);
        int injected = 0;
        while (matcher.find()) {
            if (Long.parseLong(matcher.group(2)) == DAY - 1) {
                injected++;
            }
        }
        assertThat(injected).isEqualTo(2);
        assertThat(gapSql).doesNotContain("999999");
    }

    // --- strip + empty ---------------------------------------------------------------------------

    @Test
    void stripsTimeConditionsButKeepsOtherPredicates() {
        String stripped = builder.stripTimeConditions(
                "SELECT appName FROM traffic WHERE ts >= 0 AND ts <= 100 AND region = 'eu'");
        assertThat(stripped).contains("region = 'eu'");
        assertThat(stripped).doesNotContain("ts >=");
        assertThat(stripped).doesNotContain("ts <=");
    }

    @Test
    void strippingAWhereThatIsAllTimeRemovesTheWhereEntirely() {
        String stripped = builder.stripTimeConditions(
                "SELECT appName FROM traffic WHERE ts >= 0 AND ts <= 100");
        assertThat(stripped).doesNotContainIgnoringCase("WHERE");
    }

    @Test
    void emptyGapPlanReturnsTheOriginalSqlUnchanged() {
        String original = "SELECT appName FROM traffic WHERE ts >= 0 AND ts <= 100";
        assertThat(builder.buildGapQuery(original, body())).isEqualTo(original);
    }
}
