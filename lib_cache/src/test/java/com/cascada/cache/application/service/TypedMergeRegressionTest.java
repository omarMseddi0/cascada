package com.cascada.cache.application.service;

import com.cascada.cache.domain.*;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.adapter.out.serialization.*;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class TypedMergeRegressionTest {
    private final FrameMergeService merger = new FrameMergeService(300, "ts");
    private CanonicalQueryObject query(List<String> dimensions, PostProcessing post) {
        return CanonicalQueryObject.of(HashComponents.of(dimensions, List.of("SUM(x)"), List.of()),
                new TimeRange(0, 172799), post, QueryMetadata.globalAggregate());
    }

    @Test void preservesEqualDailyTotalsAndLargeIntegers() {
        var frame = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(9007199254740993L).build();
        var result = merger.mergeAndReconstruct(List.of(frame, frame), query(List.of(), PostProcessing.none()));
        assertThat(result.rows().getFirst().get("SUM(x)")).isEqualTo(18014398509481986L);
    }

    @Test void nullAndLiteralNullRemainDifferentGroups() {
        var frame = ResultFrame.builder().column("country", ColumnType.STRING).column("SUM(x)", ColumnType.LONG)
                .row(null, 10L).row("null", 20L).build();
        var result = merger.mergeAndReconstruct(List.of(frame), query(List.of("country"), PostProcessing.none()));
        assertThat(result.rowCount()).isEqualTo(2);
        assertThat(result.rows().getFirst().get("country")).isNull();
        assertThat(result.rows().getLast().get("country")).isEqualTo("null");
    }

    @Test void ordersNumericDimensionsNumericallyAndHonorsNullPlacement() {
        var frame = ResultFrame.builder().column("id", ColumnType.LONG).column("SUM(x)", ColumnType.LONG)
                .row(10L, 1L).row(2L, 1L).row(null, 1L).build();
        var post = new PostProcessing(Optional.empty(), List.of(OrderByClause.forColumn("id", true, false)));
        var result = merger.mergeAndReconstruct(List.of(frame), query(List.of("id"), post));
        assertThat(result.rows().stream().map(r -> r.get("id")).toList()).containsExactly(2L, 10L, null);
    }

    @Test void decimalsSurviveBothSerializersAndMergeWithoutRounding() {
        BigDecimal exact = new BigDecimal("9007199254740993.123456789012345678");
        var frame = ResultFrame.builder().column("SUM(x)", ColumnType.DECIMAL).row(exact).build();
        for (var serializer : List.of(new PortableFrameSerializer(), new ArrowResultFrameSerializer())) {
            var restored = serializer.deserialize(serializer.serialize(frame));
            assertThat(restored.rows().getFirst().get("SUM(x)")).isEqualTo(exact);
            var result = merger.mergeAndReconstruct(List.of(restored, restored), query(List.of(), PostProcessing.none()));
            assertThat(result.rows().getFirst().get("SUM(x)")).isEqualTo(exact.add(exact));
        }
    }
}
