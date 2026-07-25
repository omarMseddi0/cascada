package com.cascada.sql.rewrite;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the gap-range coalescing ({@code _merge_consecutive_gaps}) and the surgical injection of
 * the gap predicate into the physical SQL ({@code _redefine_spark_query_for_gaps}).
 */
class GapQueryBuilderTest {

    private static final long DAY = 86_400L;
    private final GapQueryBuilder builder = new GapQueryBuilder("ts", DAY);

    @Test
    void mergesConsecutiveBodyDaysIntoOneContiguousRange() {
        GapPlan gapPlan = new GapPlan(Optional.empty(), List.of(0L, DAY, 2 * DAY), Optional.empty());
        List<TimeRange> merged = builder.mergeConsecutiveGaps(gapPlan);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).startTimestampSeconds()).isZero();
        assertThat(merged.get(0).endTimestampSeconds()).isEqualTo(3 * DAY - 1);
    }

    @Test
    void keepsNonAdjacentRangesSeparate() {
        GapPlan gapPlan = new GapPlan(Optional.empty(), List.of(0L, 5 * DAY), Optional.empty());
        assertThat(builder.mergeConsecutiveGaps(gapPlan)).hasSize(2);
    }

    @Test
    void mergesHeadBodyAndTailWhenAdjacent() {
        GapPlan gapPlan = new GapPlan(
                Optional.of(new TimeRange(50, DAY - 1)),
                List.of(DAY),
                Optional.of(new TimeRange(2 * DAY, 2 * DAY + 100)));
        List<TimeRange> merged = builder.mergeConsecutiveGaps(gapPlan);
        assertThat(merged).hasSize(1);
        assertThat(merged.get(0).startTimestampSeconds()).isEqualTo(50);
        assertThat(merged.get(0).endTimestampSeconds()).isEqualTo(2 * DAY + 100);
    }

    @Test
    void injectsGapPredicateAndPreservesNonTimeFilters() {
        GapPlan gapPlan = new GapPlan(Optional.empty(), List.of(0L), Optional.empty());
        String gapSql = builder.buildGapQuery(
                "SELECT appName, SUM(bytes) FROM traffic WHERE ts >= 0 AND ts <= 999999 AND region = 'eu' "
                        + "GROUP BY appName",
                gapPlan);

        assertThat(gapSql).contains("region = 'eu'");
        assertThat(gapSql).contains("ts >= 0");
        assertThat(gapSql).contains("ts <= " + (DAY - 1));
        // the original wide upper bound must be gone, replaced by the gap range
        assertThat(gapSql).doesNotContain("999999");
    }

    @Test
    void stripsTimeConditionsButKeepsOtherPredicates() {
        String stripped = builder.stripTimeConditions(
                "SELECT appName FROM traffic WHERE ts >= 0 AND ts <= 100 AND region = 'eu'");
        assertThat(stripped).contains("region = 'eu'");
        assertThat(stripped).doesNotContain("ts >=");
        assertThat(stripped).doesNotContain("ts <=");
    }

    @Test
    void emptyGapPlanReturnsTheOriginalSql() {
        String original = "SELECT appName FROM traffic WHERE ts >= 0 AND ts <= 100";
        assertThat(builder.buildGapQuery(original, new GapPlan(Optional.empty(), List.of(), Optional.empty())))
                .isEqualTo(original);
    }
}
