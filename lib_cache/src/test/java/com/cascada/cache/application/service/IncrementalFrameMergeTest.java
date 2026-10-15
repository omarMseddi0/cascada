package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.*;
import com.cascada.cache.domain.time.TimeRange;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class IncrementalFrameMergeTest {
    private CanonicalQueryObject query(List<String> dimensions, QueryMetadata metadata, PostProcessing post) {
        return new CanonicalQueryObject(HashComponents.of(dimensions, List.of("SUM(x)", "COUNT(x)"), List.of()),
                new TimeRange(-86_400, 172_799), post, metadata, "FULL_SQL", List.of("traffic"), List.of());
    }

    @Test
    void incrementalMatchesBulkForTwoMinuteStepsNegativeTimeAndOrderedPartialInputs() {
        FrameMergeService service = new FrameMergeService(120, "ts");
        CanonicalQueryObject query = query(List.of("app"), QueryMetadata.timeSeries(120), PostProcessing.none());
        List<ResultFrame> frames = new ArrayList<>();
        for (int bucket = 0; bucket < 10; bucket++) {
            ResultFrame.Builder builder = ResultFrame.builder().column("ts", ColumnType.LONG)
                    .column("app", ColumnType.STRING).column("SUM(x)", ColumnType.DOUBLE).column("COUNT(x)", ColumnType.LONG);
            for (int row = 0; row < 100; row++) builder.row((long) row * 37 - 500, row % 3 == 0 ? null : "app" + row % 5,
                    row % 9 == 0 ? null : row + bucket * .25, 1L);
            frames.add(builder.build());
        }
        FrameMergeService.IncrementalMerge incremental = service.incremental(query);
        frames.forEach(incremental::add);
        ResultFrame expected = service.mergeAndReconstruct(frames, query);
        assertThat(incremental.finish().rows()).isEqualTo(expected.rows());
        assertThat(incremental.finish().columnTypes()).isEqualTo(expected.columnTypes());
    }

    @Test
    void reconstructionAndTopKRunAfterAllBucketsAndKeepFloatingPointInputOrder() {
        FrameMergeService service = new FrameMergeService(300, "ts");
        QueryMetadata metadata = QueryMetadata.globalAggregate().withOriginalAggregates(List.of("AVG(x) AS mean"));
        CanonicalQueryObject query = query(List.of("app"), metadata,
                new PostProcessing(Optional.of(2), List.of(OrderByClause.forColumn("mean", false, false))));
        List<ResultFrame> frames = List.of(frame("a", 1.0e16, 1), frame("a", -1.0e16, 1), frame("a", 1.0, 1),
                frame("b", 12.0, 2), frame("c", 4.0, 1));
        FrameMergeService.IncrementalMerge incremental = service.incremental(query);
        frames.forEach(incremental::add);
        assertThat(incremental.finish().rows()).isEqualTo(service.mergeAndReconstruct(frames, query).rows());
        assertThat(incremental.finish().rows()).extracting(row -> row.get("app")).containsExactly("b", "c");
    }

    @Test
    void zeroRowSchemasAreCheckedAndErrorsAreDeferredUntilFinish() {
        FrameMergeService service = new FrameMergeService(300, "ts");
        CanonicalQueryObject query = query(List.of("app"), QueryMetadata.globalAggregate(), PostProcessing.none());
        ResultFrame empty = ResultFrame.builder().column("app", ColumnType.STRING).column("SUM(x)", ColumnType.DOUBLE)
                .column("COUNT(x)", ColumnType.LONG).build();
        FrameMergeService.IncrementalMerge allEmpty = service.incremental(query);
        allEmpty.add(empty); allEmpty.add(empty);
        assertThat(allEmpty.finish()).isSameAs(empty);
        FrameMergeService.IncrementalMerge mismatched = service.incremental(query);
        mismatched.add(frame("a", 1.0, 1));
        mismatched.add(ResultFrame.builder().column("other", ColumnType.STRING).build());
        assertThatThrownBy(mismatched::finish).isInstanceOf(IllegalArgumentException.class);
    }

    private ResultFrame frame(String app, double sum, long count) {
        return ResultFrame.builder().column("app", ColumnType.STRING).column("SUM(x)", ColumnType.DOUBLE)
                .column("COUNT(x)", ColumnType.LONG).row(app, sum, count).build();
    }
}
