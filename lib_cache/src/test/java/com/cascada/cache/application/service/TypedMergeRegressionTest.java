package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.OrderByClause;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.TimeRange;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypedMergeRegressionTest {
    private final FrameMergeService merger = new FrameMergeService(300, "ts");
    private CanonicalQueryObject query(List<String> dimensions, PostProcessing post) {
        return query(dimensions, List.of("SUM(x)"), post);
    }

    private CanonicalQueryObject query(List<String> dimensions, List<String> aggregates, PostProcessing post) {
        return CanonicalQueryObject.of(HashComponents.of(dimensions, aggregates, List.of()),
                new TimeRange(0, 172799), post, QueryMetadata.globalAggregate().withAggregateSpecs(aggregates));
    }

    @Test void preservesEqualDailyTotalsAndLargeIntegers() {
        var frame = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(9007199254740993L).build();
        var result = merger.mergeAndReconstruct(List.of(frame, frame), query(List.of(), PostProcessing.none()));
        assertThat(result.rows().get(0).get("SUM(x)")).isEqualTo(18014398509481986L);
    }

    @Test void nullAndLiteralNullRemainDifferentGroups() {
        var frame = ResultFrame.builder().column("country", ColumnType.STRING).column("SUM(x)", ColumnType.LONG)
                .row(null, 10L).row("null", 20L).build();
        var result = merger.mergeAndReconstruct(List.of(frame), query(List.of("country"), PostProcessing.none()));
        assertThat(result.rowCount()).isEqualTo(2);
        assertThat(result.rows().get(0).get("country")).isNull();
        assertThat(result.rows().get(1).get("country")).isEqualTo("null");
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
            assertThat(restored.rows().get(0).get("SUM(x)")).isEqualTo(exact);
            var result = merger.mergeAndReconstruct(List.of(restored, restored), query(List.of(), PostProcessing.none()));
            assertThat(result.rows().get(0).get("SUM(x)")).isEqualTo(exact.add(exact));
        }
    }

    @Test void stringMinMaxColumnsRemainInTheMergedResult() {
        var firstPartial = ResultFrame.builder().column("first_city", ColumnType.STRING).column("total", ColumnType.LONG)
                .row("Zurich", 3L).build();
        var secondPartial = ResultFrame.builder().column("first_city", ColumnType.STRING).column("total", ColumnType.LONG)
                .row("Amsterdam", 4L).build();
        var query = query(List.of(), List.of("MIN(city) AS first_city", "SUM(x) AS total"), PostProcessing.none());

        var result = merger.mergeAndReconstruct(List.of(firstPartial, secondPartial), query);

        assertThat(result.columnNames()).containsExactly("first_city", "total");
        assertThat(result.columnType("first_city")).isEqualTo(ColumnType.STRING);
        assertThat(result.rows().get(0)).containsEntry("first_city", "Amsterdam").containsEntry("total", 7L);
    }

    @Test void stringMinUsesSparkCodePointOrderingForSupplementaryCharacters() {
        var bmpStringPartial = ResultFrame.builder().column("first_city", ColumnType.STRING).row("\uE000").build();
        var supplementaryStringPartial = ResultFrame.builder().column("first_city", ColumnType.STRING)
                .row("\uD83D\uDE00").build();
        var query = query(List.of(), List.of("MIN(city) AS first_city"), PostProcessing.none());

        var result = merger.mergeAndReconstruct(List.of(bmpStringPartial, supplementaryStringPartial), query);

        // UTF-8/code-point order places U+E000 before U+1F600; Java UTF-16 String.compareTo does not.
        assertThat(result.rows().get(0).get("first_city")).isEqualTo("\uE000");
    }

    @Test void unsupportedStringAggregateFailsInsteadOfDroppingItsColumn() {
        var frame = ResultFrame.builder().column("median_city", ColumnType.STRING).row("Amsterdam").build();
        var query = query(List.of(), List.of("MEDIAN(city) AS median_city"), PostProcessing.none());

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(frame), query))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unsupported STRING aggregate column 'median_city'");
    }

    @Test void emptyInputsPreserveTheirKnownSchema() {
        var empty = ResultFrame.builder().column("country", ColumnType.STRING)
                .column("first_city", ColumnType.STRING).column("total", ColumnType.LONG).build();
        var query = query(List.of("country"), List.of("MIN(city) AS first_city", "SUM(x) AS total"),
                PostProcessing.none());

        var result = merger.mergeAndReconstruct(List.of(empty), query);

        assertThat(result.isEmpty()).isTrue();
        assertThat(result.columnNames()).containsExactly("country", "first_city", "total");
        assertThat(result.columnTypes()).containsEntry("country", ColumnType.STRING)
                .containsEntry("first_city", ColumnType.STRING).containsEntry("total", ColumnType.LONG);
    }

    @Test void rejectsSchemaMismatchEvenWhenTheMismatchingFrameIsEmpty() {
        var populated = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(3L).build();
        var incompatibleEmpty = ResultFrame.builder().column("SUM(y)", ColumnType.LONG).build();

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(populated, incompatibleEmpty),
                query(List.of(), PostProcessing.none())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column names/order");
    }

    @Test void rejectsSchemaTypeChangesInsteadOfTruncatingValues() {
        var integral = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(3L).build();
        var floating = ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).row(4.5).build();

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(integral, floating),
                query(List.of(), PostProcessing.none())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("column 'SUM(x)' has type DOUBLE, expected LONG");
    }

    @Test void floatingMinAndMaxTreatNanLikeSparkOrdering() {
        var firstPartial = ResultFrame.builder().column("min_value", ColumnType.DOUBLE)
                .column("max_value", ColumnType.DOUBLE).row(2.0, 2.0).build();
        var secondPartial = ResultFrame.builder().column("min_value", ColumnType.DOUBLE)
                .column("max_value", ColumnType.DOUBLE).row(Double.NaN, Double.NaN).build();
        var query = query(List.of(), List.of("MIN(x) AS min_value", "MAX(x) AS max_value"), PostProcessing.none());

        var result = merger.mergeAndReconstruct(List.of(firstPartial, secondPartial), query);

        assertThat(result.rows().get(0).get("min_value")).isEqualTo(2.0);
        assertThat((Double) result.rows().get(0).get("max_value")).isNaN();
    }

    @Test void decimalGroupingIgnoresScaleDifferencesWhilePreservingTheFirstTypedValue() {
        var firstPartial = ResultFrame.builder().column("id", ColumnType.DECIMAL).column("SUM(x)", ColumnType.LONG)
                .row(new BigDecimal("1.0"), 2L).build();
        var secondPartial = ResultFrame.builder().column("id", ColumnType.DECIMAL).column("SUM(x)", ColumnType.LONG)
                .row(new BigDecimal("1.00"), 3L).build();

        var result = merger.mergeAndReconstruct(List.of(firstPartial, secondPartial),
                query(List.of("id"), PostProcessing.none()));

        assertThat(result.rowCount()).isEqualTo(1);
        assertThat(result.rows().get(0).get("id")).isEqualTo(new BigDecimal("1.0"));
        assertThat(result.rows().get(0).get("SUM(x)")).isEqualTo(5L);
    }

    @Test void groupedMergePreservesInputColumnOrder() {
        var frame = ResultFrame.builder().column("SUM(x)", ColumnType.LONG)
                .column("country", ColumnType.STRING).row(5L, "FR").build();

        var result = merger.mergeAndReconstruct(List.of(frame),
                query(List.of("country"), PostProcessing.none()));

        assertThat(result.columnNames()).containsExactly("SUM(x)", "country");
    }

    @Test void decimalSumBeyondSparkPrecisionRejectsTheCacheMerge() {
        var largestPrecision38 = new BigDecimal("99999999999999999999999999999999999999");
        var firstPartial = ResultFrame.builder().column("SUM(x)", ColumnType.DECIMAL).row(largestPrecision38).build();
        var secondPartial = ResultFrame.builder().column("SUM(x)", ColumnType.DECIMAL).row(BigDecimal.ONE).build();

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(firstPartial, secondPartial),
                query(List.of(), PostProcessing.none())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("result precision 39 exceeds Spark's maximum precision of 38");
    }

    @Test void decimalSumRejectsNegativeScaleValuesThatExceedSparkPrecision() {
        var scientific = ResultFrame.builder().column("SUM(x)", ColumnType.DECIMAL)
                .row(new BigDecimal("1E+39")).build();

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(scientific),
                query(List.of(), PostProcessing.none())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("result precision 40 exceeds Spark's maximum precision of 38");
    }

    @Test void longSumOverflowRejectsTheCacheMergeForSparkFallback() {
        var largestLongPartial = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(Long.MAX_VALUE).build();
        var oneLongPartial = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).row(1L).build();

        assertThatThrownBy(() -> merger.mergeAndReconstruct(List.of(largestLongPartial, oneLongPartial),
                query(List.of(), PostProcessing.none())))
                .isInstanceOf(ArithmeticException.class);
    }
}
