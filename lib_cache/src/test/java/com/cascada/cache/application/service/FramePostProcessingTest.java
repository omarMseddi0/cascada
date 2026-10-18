package com.cascada.cache.application.service;

import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.OrderByClause;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.time.TimeRange;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.assertThat;

class FramePostProcessingTest {
    private final FrameMergeService merge = new FrameMergeService(300, "ts");

    private CanonicalQueryObject query(PostProcessing processing) {
        return new CanonicalQueryObject(HashComponents.of(List.of("id"), List.of("SUM(x)", "SUM(y)"), List.of()),
                new TimeRange(0, 86399), processing, QueryMetadata.globalAggregate(), "SQL", List.of(), List.of());
    }

    @Test
    void boundedTopKMatchesFullStableSortWithMultipleKeysNullsAndTies() {
        ResultFrame.Builder builder = ResultFrame.builder().column("id", ColumnType.LONG)
                .column("SUM(x)", ColumnType.DOUBLE).column("SUM(y)", ColumnType.LONG);
        Random random = new Random(42);
        for (int row = 0; row < 400; row++) {
            builder.appendLong(row);
            if (row % 11 == 0) builder.appendNull(); else builder.appendDouble(random.nextInt(5));
            builder.appendLong(random.nextInt(4));
        }
        ResultFrame input = builder.build();
        for (boolean ascending : List.of(true, false)) {
            for (boolean nullsFirst : List.of(true, false)) {
                List<OrderByClause> order = List.of(OrderByClause.forColumn("SUM(x)", ascending, nullsFirst),
                        OrderByClause.forColumn("SUM(y)", false, false));
                ResultFrame sorted = merge.mergeAndReconstruct(List.of(input), query(new PostProcessing(Optional.empty(), order)));
                for (int limit : List.of(0, 1, 17, 399, 400, 500)) {
                    ResultFrame top = merge.mergeAndReconstruct(List.of(input), query(new PostProcessing(Optional.of(limit), order)));
                    assertThat(top.rows()).containsExactlyElementsOf(sorted.rows().subList(0, Math.min(limit, 400)));
                    assertThat(top.columnTypes()).isEqualTo(sorted.columnTypes());
                }
            }
        }
    }

    @Test
    void limitAlonePreservesDeterministicGroupOrderAndSchema() {
        ResultFrame input = ResultFrame.builder().column("id", ColumnType.LONG).column("SUM(x)", ColumnType.DOUBLE)
                .row(3L, 30.0).row(1L, 10.0).row(2L, 20.0).build();
        ResultFrame limited = merge.mergeAndReconstruct(List.of(input), query(new PostProcessing(Optional.of(2), List.of())));
        assertThat(limited.rows()).extracting(row -> row.get("id")).containsExactly(1L, 2L);
    }

    @Test void fullSortAndTopKUseSparkUnicodeOrderWithNulls() {
        var input = ResultFrame.builder().column("label", ColumnType.STRING).column("SUM(x)", ColumnType.LONG)
                .row("😀", 1L).row("\uE000", 2L).row("z", 3L).row(null, 4L).build();
        for (boolean ascending : List.of(true, false)) {
            List<Object> expected = ascending ? Arrays.asList(null, "z", "\uE000", "😀")
                    : Arrays.asList("😀", "\uE000", "z", null);
            for (int limit : List.of(2, 4)) {
                var canonical = CanonicalQueryObject.of(HashComponents.of(List.of("label"), List.of("SUM(x)"), List.of()),
                        new TimeRange(0, 86399), new PostProcessing(Optional.of(limit),
                                List.of(OrderByClause.forColumn("label", ascending, ascending))), QueryMetadata.globalAggregate());
                var actual = merge.mergeAndReconstruct(List.of(input), canonical);
                assertThat(actual.rows()).extracting(row -> row.get("label")).containsExactlyElementsOf(expected.subList(0, limit));
            }
        }
    }
}
