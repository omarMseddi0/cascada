package com.cascada.sql.rewrite;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based invariants for {@code _merge_consecutive_gaps}: whatever body days are thrown at it,
 * the coalescer must return strictly ordered, genuinely non-adjacent ranges that together cover exactly
 * the same seconds as the input day-buckets — never more, never fewer.
 */
class GapMergePropertyTest {

    private static final long DAY = 86_400L;
    private final GapQueryBuilder builder = new GapQueryBuilder("ts", DAY);

    @Property(tries = 500)
    void mergedRangesAreOrderedNonAdjacentAndCountNeverGrows(
            @ForAll @Size(min = 1, max = 12) List<@IntRange(min = 0, max = 20) Integer> dayIndices) {
        List<Long> body = dayIndices.stream().distinct().map(day -> day * DAY).toList();
        List<TimeRange> merged = builder.mergeConsecutiveGaps(
                new GapPlan(Optional.empty(), body, Optional.empty()));

        // never more ranges than inputs
        assertThat(merged.size()).isLessThanOrEqualTo(body.size());

        for (int i = 0; i < merged.size(); i++) {
            assertThat(merged.get(i).startTimestampSeconds())
                    .isLessThanOrEqualTo(merged.get(i).endTimestampSeconds());
            if (i > 0) {
                // strictly ordered AND a real gap (> 1s) between consecutive merged ranges
                assertThat(merged.get(i).startTimestampSeconds())
                        .isGreaterThan(merged.get(i - 1).endTimestampSeconds() + 1);
            }
        }

        // coverage is exactly the union of the input day-buckets
        TreeSet<Long> coveredByMerge = secondsCoveredByDayBuckets(body);
        for (TimeRange range : merged) {
            for (long second : sampleSeconds(range)) {
                assertThat(coveredByMerge).contains(second);
            }
        }
    }

    private TreeSet<Long> secondsCoveredByDayBuckets(List<Long> body) {
        TreeSet<Long> seconds = new TreeSet<>();
        for (long dayStart : body) {
            seconds.add(dayStart);
            seconds.add(dayStart + DAY - 1);
        }
        return seconds;
    }

    /** Sample the endpoints of a merged range; both must fall inside some input day-bucket. */
    private List<Long> sampleSeconds(TimeRange range) {
        return List.of(range.startTimestampSeconds(), range.endTimestampSeconds());
    }
}
