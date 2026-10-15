package com.cascada.sql.adapter.calcite;

import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

import java.util.List;
import java.util.Optional;

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

        // Compare normalized interval unions, so a missing interior day or extra range fails too.
        assertThat(merged).containsExactlyElementsOf(normalize(dayBuckets(body)));
    }

    private List<TimeRange> dayBuckets(List<Long> starts) {
        return starts.stream().map(start -> new TimeRange(start, start + DAY - 1)).toList();
    }

    private List<TimeRange> normalize(List<TimeRange> ranges) {
        List<TimeRange> sorted = ranges.stream()
                .sorted((left, right) -> Long.compare(left.startTimestampSeconds(), right.startTimestampSeconds()))
                .toList();
        List<TimeRange> normalized = new java.util.ArrayList<>();
        for (TimeRange current : sorted) {
            if (normalized.isEmpty()) {
                normalized.add(current);
                continue;
            }
            TimeRange previous = normalized.get(normalized.size() - 1);
            if (previous.endTimestampSeconds() + 1 >= current.startTimestampSeconds()) {
                normalized.set(normalized.size() - 1, new TimeRange(previous.startTimestampSeconds(),
                        Math.max(previous.endTimestampSeconds(), current.endTimestampSeconds())));
            } else {
                normalized.add(current);
            }
        }
        return normalized;
    }
}
