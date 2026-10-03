package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.*;
import com.cascada.cache.domain.time.TimeRange;
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
}
