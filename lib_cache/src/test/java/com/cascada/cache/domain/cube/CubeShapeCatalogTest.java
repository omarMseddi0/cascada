package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.TimeRange;
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
        QueryShape fine = new QueryShape(Set.of("country"), Set.of(), Set.of("MAX(x)"), Set.of("traffic"),
                Map.of("peak", AggregateFunction.MAXIMUM));
        QueryShape coarse = new QueryShape(Set.of(), Set.of(), Set.of("MAX(x)"), Set.of("traffic"),
                Map.of("peak", AggregateFunction.MAXIMUM));
        ResultFrame frame = ResultFrame.builder().column("country", ColumnType.STRING).column("peak", ColumnType.LONG)
                .row("FR", 10L).row("US", 20L).build();
        catalog.register(WINDOW, fine, frame);

        Optional<ResultFrame> answer = catalog.tryAnswer(WINDOW, coarse);

        assertThat(answer).isPresent();
        assertThat(answer.get().rows().get(0).get("peak")).isEqualTo(20.0);
    }

    @Test
    void refusesNullMeasuresInsteadOfThrowing() {
        QueryShape shape = new QueryShape(Set.of(), Set.of(), Set.of("SUM(x)"));
        ResultFrame nullSum = ResultFrame.builder().column("SUM(x)", ColumnType.LONG).appendNull().build();
        catalog.register(WINDOW, shape, nullSum);

        assertThat(catalog.tryAnswer(WINDOW, shape)).isEmpty();
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
