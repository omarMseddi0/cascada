package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import com.cascada.cache.domain.merge.AggregateFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The catalog seam between the engine and the subsumption algebra: exact-window isolation, the
 * verifier gate, LRU bounding, dedup, and the flush hook.
 */
class CubeShapeCatalogTest {

    private static final TimeRange WINDOW = new TimeRange(0, 86_399);
    private static final TimeRange OTHER_WINDOW = new TimeRange(0, 172_799);

    private final CubeShapeCatalog catalog = new CubeShapeCatalog();

    private ResultFrame fineFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(bytes)", 10.0))
                .row(Map.of("appName", "netflix", "deviceType", "tablet", "SUM(bytes)", 5.0))
                .build();
    }

    private QueryShape fineShape() {
        return new QueryShape(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(bytes)"));
    }

    private QueryShape coarseShape() {
        return new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));
    }

    @Test
    void answersACoarserQueryFromARegisteredFinerShape() {
        catalog.register(WINDOW, fineShape(), fineFrame());

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarseShape());

        assertThat(answer).isPresent();
        assertThat(answer.get().rowCount()).isEqualTo(1);
        assertThat(((Number) answer.get().rows().get(0).get("SUM(bytes)")).doubleValue()).isEqualTo(15.0);
    }

    @Test
    void aDifferentTimeWindowNeverSeesTheEntry() {
        catalog.register(WINDOW, fineShape(), fineFrame());

        assertThat(catalog.tryAnswer(OTHER_WINDOW, coarseShape())).isEmpty();
    }

    @Test
    void emptyFramesAreNeverRegistered() {
        catalog.register(WINDOW, fineShape(), ResultFrame.empty());

        assertThat(catalog.windowCount()).isZero();
        assertThat(catalog.tryAnswer(WINDOW, coarseShape())).isEmpty();
    }

    @Test
    void reRegisteringTheSameShapeForTheSameWindowIsANoOp() {
        catalog.register(WINDOW, fineShape(), fineFrame());
        ResultFrame tampered = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(bytes)", 999.0))
                .build();
        catalog.register(WINDOW, fineShape(), tampered);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarseShape());
        assertThat(answer).isPresent();
        assertThat(((Number) answer.get().rows().get(0).get("SUM(bytes)")).doubleValue()).isEqualTo(15.0);
    }

    @Test
    void aRollUpTheVerifierCannotProveIsRefused() {
        // shape CLAIMS SUM+COUNT were stored, but the frame physically lacks the COUNT column —
        // an AVG query then subsumes statically yet cannot be reconstructed; the verifier must veto.
        QueryShape claimsSumAndCount = new QueryShape(Set.of("appName", "deviceType"), Set.of(),
                Set.of("SUM(latency)", "COUNT(latency)"));
        ResultFrame missingCount = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "deviceType", "mobile", "SUM(latency)", 100.0))
                .build();
        catalog.register(WINDOW, claimsSumAndCount, missingCount);

        QueryShape avgQuery = new QueryShape(Set.of("appName"), Set.of(), Set.of("AVG(latency)"));

        assertThat(catalog.tryAnswer(WINDOW, avgQuery)).isEmpty();
    }

    @Test
    void preservesAnAliasedMaximumDuringRollUp() {
        QueryShape fine = new QueryShape(Set.of("country"), Set.of(), Set.of("MAX(x)"), Set.of(),
                Map.of("peak", AggregateFunction.MAXIMUM));
        QueryShape coarse = new QueryShape(Set.of(), Set.of(), Set.of("MAX(x)"), Set.of(),
                Map.of("peak", AggregateFunction.MAXIMUM));
        ResultFrame frame = ResultFrame.builder().column("country", ColumnType.STRING).column("peak", ColumnType.LONG)
                .row("FR", 10L).row("US", 20L).build();
        catalog.register(WINDOW, fine, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarse);

        assertThat(answer).isPresent();
        assertThat(answer.get().columnType("peak")).isEqualTo(ColumnType.LONG);
        assertThat(answer.get().rows().get(0).get("peak")).isEqualTo(20L);
    }

    @Test
    void preservesAnAllNullMeasureInsteadOfTreatingNullAsMissingState() {
        QueryShape shape = new QueryShape(Set.of(), Set.of(), Set.of("SUM(x)"));
        ResultFrame nullSum = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).appendNull().build();
        catalog.register(WINDOW, shape, nullSum);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, shape);

        assertThat(answer).isPresent();
        assertThat(answer.get().columnType("SUM(x)")).isEqualTo(ColumnType.LONG);
        assertThat(answer.get().rows().get(0).get("SUM(x)")).isNull();
    }

    @Test
    void bypassesCubeWhenLongSumOverflowsWithoutKnownSparkAnsiMode() {
        ResultFrame overflowFrame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("Paris", "north", Long.MAX_VALUE)
                .row("Paris", "south", 1L)
                .build();
        QueryShape finer = new QueryShape(Set.of("city", "region"), Set.of(), Set.of("SUM(x)"));
        QueryShape coarser = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x)"));
        catalog.register(WINDOW, finer, overflowFrame);

        assertThat(catalog.tryAnswer(WINDOW, coarser)).isEmpty();
    }

    @Test
    void preservesNanAsAValidMinAndMaxValue() {
        ResultFrame nanFrame = ResultFrame.builder()
                .column("app", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("MIN(x)", ColumnType.DOUBLE)
                .column("MAX(x)", ColumnType.DOUBLE)
                .row("video", "north", Double.NaN, Double.NaN)
                .row("video", "south", Double.NaN, Double.NaN)
                .row("audio", "north", 7.0, 7.0)
                .row("audio", "south", Double.NaN, Double.NaN)
                .build();
        QueryShape finer = new QueryShape(Set.of("app", "region"), Set.of(), Set.of("MIN(x)", "MAX(x)"));
        QueryShape coarser = new QueryShape(Set.of("app"), Set.of(), Set.of("MIN(x)", "MAX(x)"));
        catalog.register(WINDOW, finer, nanFrame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarser);

        assertThat(answer).isPresent();
        Map<String, Map<String, Object>> rowsByApp = new java.util.HashMap<>();
        answer.get().rows().forEach(row -> rowsByApp.put((String) row.get("app"), row));
        assertThat((Double) rowsByApp.get("video").get("MIN(x)")).isNaN();
        assertThat((Double) rowsByApp.get("video").get("MAX(x)")).isNaN();
        assertThat(rowsByApp.get("audio").get("MIN(x)")).isEqualTo(7.0);
        assertThat((Double) rowsByApp.get("audio").get("MAX(x)")).isNaN();
    }

    @Test
    void filtersLongDimensionsExactlyAboveDoublePrecisionThroughCatalog() {
        ResultFrame frame = ResultFrame.builder()
                .column("id", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .column("SUM(x)", ColumnType.DOUBLE)
                .row(9_007_199_254_740_993L, "wrong", 10.0)
                .row(9_007_199_254_740_992L, "right", 20.0)
                .build();
        QueryShape finer = new QueryShape(Set.of("id", "city"), Set.of(), Set.of("SUM(x)"));
        QueryShape filtered = new QueryShape(Set.of("city"), Set.of("id = 9007199254740992"),
                Set.of("SUM(x)"));
        catalog.register(WINDOW, finer, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, filtered);

        assertThat(answer).isPresent();
        assertThat(answer.get().rowCount()).isEqualTo(1);
        assertThat(answer.get().rows().get(0).get("city")).isEqualTo("right");
    }

    @Test
    void keepsNullCitySeparateFromTheStringNullThroughCatalog() {
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
        QueryShape finer = new QueryShape(Set.of("city", "device"), Set.of(), Set.of("SUM(x)"));
        QueryShape coarser = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x)"));
        catalog.register(WINDOW, finer, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarser);

        assertThat(answer).isPresent();
        assertThat(answer.get().rowCount()).isEqualTo(2);
        assertThat(answer.get().rows()).anySatisfy(row -> {
            assertThat(row.get("city")).isNull();
            assertThat(row.get("SUM(x)")).isEqualTo(10.0);
        });
        assertThat(answer.get().rows()).anySatisfy(row -> {
            assertThat(row.get("city")).isEqualTo("null");
            assertThat(row.get("SUM(x)")).isEqualTo(20.0);
        });
    }

    @Test
    void preservesLongMeasurePrecisionThroughCatalog() {
        ResultFrame frame = ResultFrame.builder()
                .column("city", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("SUM(x)", ColumnType.LONG)
                .row("Paris", "north", 9_007_199_254_740_992L)
                .row("Paris", "south", 1L)
                .build();
        QueryShape finer = new QueryShape(Set.of("city", "region"), Set.of(), Set.of("SUM(x)"));
        QueryShape coarser = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x)"));
        catalog.register(WINDOW, finer, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarser);

        assertThat(answer).isPresent();
        assertThat(answer.get().columnType("SUM(x)")).isEqualTo(ColumnType.LONG);
        assertThat(answer.get().rows().get(0).get("SUM(x)")).isEqualTo(9_007_199_254_740_993L);
    }

    @Test
    void returnsProjectedColumnsInTheCanonicalOrder() {
        List<String> projection = List.of("SUM(x) AS total", "city");
        ResultFrame frame = ResultFrame.builder()
                .column("total", ColumnType.LONG)
                .column("city", ColumnType.STRING)
                .row(15L, "Paris")
                .build();
        QueryShape finer = new QueryShape(Set.of("city", "region"), Set.of(), Set.of("SUM(x) AS total"),
                Set.of("traffic"), Map.of("total", AggregateFunction.SUM), projection);
        QueryShape coarser = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(x) AS total"),
                Set.of("traffic"), Map.of("total", AggregateFunction.SUM), projection);
        catalog.register(WINDOW, finer, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarser);

        assertThat(answer).isPresent();
        assertThat(answer.get().columnNames()).containsExactly("total", "city");
        assertThat(answer.get().columnType("total")).isEqualTo(ColumnType.LONG);
    }

    @Test
    void leastRecentlyUsedWindowIsEvictedAtTheBound() {
        CubeShapeCatalog bounded = new CubeShapeCatalog(2);
        TimeRange third = new TimeRange(0, 259_199);
        bounded.register(WINDOW, fineShape(), fineFrame());
        bounded.register(OTHER_WINDOW, fineShape(), fineFrame());
        bounded.register(third, fineShape(), fineFrame());

        assertThat(bounded.windowCount()).isEqualTo(2);
        assertThat(bounded.tryAnswer(WINDOW, coarseShape())).isEmpty(); // the eldest fell out
        assertThat(bounded.tryAnswer(third, coarseShape())).isPresent();
    }

    @Test
    void boundsTheNumberOfShapesWithinOneFrequentlyUsedWindow() {
        CubeShapeCatalog bounded = new CubeShapeCatalog(2, 2);
        bounded.register(WINDOW, fineShape(), fineFrame());
        bounded.register(WINDOW, coarseShape(), fineFrame());
        bounded.register(WINDOW, new QueryShape(Set.of("deviceType"), Set.of(), Set.of("SUM(bytes)")), fineFrame());

        assertThat(bounded.entryCount()).isEqualTo(2);
    }

    @Test
    void clearDropsEveryWindow() {
        catalog.register(WINDOW, fineShape(), fineFrame());
        catalog.register(OTHER_WINDOW, fineShape(), fineFrame());

        catalog.clear();

        assertThat(catalog.windowCount()).isZero();
        assertThat(catalog.tryAnswer(WINDOW, coarseShape())).isEmpty();
    }

    @Test
    void shapeOfMirrorsTheHashComponents() {
        HashComponents components = HashComponents.of(
                List.of("appName"), List.of("SUM(bytes)"), List.of("appName = 'netflix'"));

        QueryShape shape = CubeShapeCatalog.shapeOf(components);

        assertThat(shape.groupBy()).containsExactly("appName");
        assertThat(shape.aggregates()).containsExactly("SUM(bytes)");
        assertThat(shape.filters()).containsExactly("appName = 'netflix'");
    }

    @Test
    void rejectsANonPositiveWindowBound() {
        assertThatThrownBy(() -> new CubeShapeCatalog(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
