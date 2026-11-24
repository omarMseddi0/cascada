package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.application.model.query.CubeCoverEdge;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static com.cascada.cache.domain.model.cube.CubeMeasure.COUNT;
import static com.cascada.cache.domain.model.cube.CubeMeasure.SUM;
import static org.assertj.core.api.Assertions.assertThat;

final class ExactCubePlannerTest {
    private static final CubePopulation POPULATION = new CubePopulation(
            new ExactCubeSource("path", "table", 0, "schema", "semantics", "scope",
                    Map.of("region", "string", "device", "bigint")), "ts", "", "amount");
    private static final CubeInterval WINDOW = new CubeInterval(0, 9);

    @Test void independentCoverageAndSharedReadPricing() {
        var sum = fragment("sum", 0, 6, List.of("region", "device"), Set.of(SUM), 80);
        var count = fragment("count", 0, 9, List.of("region"), Set.of(COUNT), 20);
        var query = query(Set.of(SUM, COUNT), List.of("region"), WINDOW);
        var selection = ExactCubePlanner.select(query, POPULATION, List.of(sum, count, sum), 1).orElseThrow();
        assertThat(selection.covers().get(SUM)).containsExactly(
                new CubeCoverEdge(new CubeInterval(0, 6), sum), new CubeCoverEdge(new CubeInterval(6, 9), null));
        assertThat(selection.covers().get(COUNT)).containsExactly(new CubeCoverEdge(WINDOW, count));
        assertThat(ExactCubePlanChecker.valid(query, POPULATION, selection, 1)).isTrue();

        var a = fragment("a", 0, 9, List.of("region"), Set.of(SUM), 10);
        var b = fragment("b", 0, 9, List.of("region"), Set.of(COUNT), 10);
        var combined = fragment("ab", 0, 9, List.of("region"), Set.of(SUM, COUNT), 100);
        var shared = ExactCubePlanner.select(query, POPULATION, List.of(a, b, combined), 1).orElseThrow();
        assertThat(shared.covers().values()).containsOnly(List.of(new CubeCoverEdge(WINDOW, combined)));
    }

    @Test void rejectsInvalidCoverageAndIncompatibleNearMatches() {
        var query = query(Set.of(SUM), List.of("region"), WINDOW);
        var head = fragment("head", 0, 6, List.of("region"), Set.of(SUM), 10);
        var overlap = fragment("overlap", 5, 9, List.of("region"), Set.of(SUM), 10);
        var unsplittable = fragment("whole", 0, 10, List.of("region"), Set.of(SUM), 10);
        assertThat(ExactCubePlanChecker.valid(query, POPULATION, new ExactCubePlan(Map.of(
                SUM, List.of(new CubeCoverEdge(head.interval(), head), new CubeCoverEdge(overlap.interval(), overlap)))), 1)).isFalse();
        assertThat(ExactCubePlanChecker.valid(query, POPULATION, new ExactCubePlan(Map.of(
                SUM, List.of(new CubeCoverEdge(head.interval(), head)))), 1)).isFalse();
        assertThat(ExactCubePlanner.select(query, POPULATION, List.of(unsplittable), 1)).isEmpty();
        assertThat(ExactCubePlanner.select(query(Set.of(SUM), List.of("region", "device"), WINDOW),
                POPULATION, List.of(head), 1)).isEmpty();
        var wrong = new CubePopulation(POPULATION.source(), "ts", "device IS NOT NULL", "amount");
        assertThat(ExactCubePlanner.select(query, wrong, List.of(head), 1)).isEmpty();
        assertThat(ExactCubePlanner.select(query, POPULATION, List.of(head), 3)).isPresent();
        assertThat(ExactCubePlanner.select(query, POPULATION, List.of(head), 4)).isEmpty();
    }

    @Test void singleIngredientPlansMatchExhaustiveIntervalCovers() {
        var random = new Random(640_025);
        var window = new CubeInterval(0, 4);
        var query = query(Set.of(SUM), List.of("region"), window);
        for (int iteration = 0; iteration < 80; iteration++) {
            var candidates = new ArrayList<ExactCubeFragment>();
            for (int start = 0; start < 4; start++) {
                for (int end = start + 1; end <= 4; end++) {
                    if (random.nextBoolean()) candidates.add(fragment(start + "-" + end, start, end,
                            List.of("region"), Set.of(SUM), random.nextInt(1, 1000)));
                }
            }
            double optimal = exhaustive(candidates, 0, false);
            var result = ExactCubePlanner.select(query, POPULATION, candidates, 1);
            double actual = result.map(selected -> selected.covers().get(SUM).stream().mapToDouble(edge ->
                    edge.fragment() == null ? 1 + edge.interval().end() - edge.interval().start()
                            : cacheCost(edge.fragment())).sum()).orElse(5.0);
            assertThat(actual).as("oracle iteration %d", iteration).isCloseTo(optimal,
                    org.assertj.core.data.Offset.offset(0.000000001));
        }
    }

    @Test void denseMillionDescriptorInputCanUseACover() {
        var fragment = fragment("dense", 0, 9, List.of("region"), Set.of(SUM), 10);
        var dense = Collections.nCopies(1_000_000, fragment);
        var query = query(Set.of(SUM), List.of("region"), WINDOW);
        var plan = ExactCubePlanner.select(query, POPULATION, dense, 1).orElseThrow();
        assertThat(plan.covers().get(SUM)).containsExactly(new CubeCoverEdge(WINDOW, fragment));
    }

    @Test void sharedCoverCanReadPastTheFormerCandidateAndFragmentCutoffs() {
        var candidates = new ArrayList<ExactCubeFragment>();
        for (int day = 0; day < 96; day++) {
            candidates.add(fragment("day-" + day, day, day + 1, List.of("region"), Set.of(SUM, COUNT), 10));
        }
        var query = query(Set.of(SUM, COUNT), List.of("region"), new CubeInterval(0, 96));
        var plan = ExactCubePlanner.select(query, POPULATION, candidates, 1).orElseThrow();
        assertThat(plan.covers().get(SUM)).hasSize(96).isEqualTo(plan.covers().get(COUNT));
        assertThat(plan.covers().get(SUM).stream().map(CubeCoverEdge::fragment)).containsExactlyElementsOf(candidates);
    }

    private static double exhaustive(List<ExactCubeFragment> candidates, long start, boolean previousResidual) {
        if (start == 4) return 0;
        double best = Double.POSITIVE_INFINITY;
        for (var candidate : candidates) {
            if (candidate.interval().start() == start) {
                best = Math.min(best, cacheCost(candidate) + exhaustive(candidates, candidate.interval().end(), false));
            }
        }
        if (!previousResidual) {
            for (long end = start + 1; end <= 4; end++) {
                best = Math.min(best, 1 + end - start + exhaustive(candidates, end, true));
            }
        }
        return best;
    }

    private static double cacheCost(ExactCubeFragment fragment) {
        return 0.02 + fragment.decodedBytes() / (double) (8 * 1024 * 1024);
    }

    private static ExactCubeQuery query(Set<CubeMeasure> measures, List<String> grain, CubeInterval interval) {
        return new ExactCubeQuery("path", "ts", interval, "", grain, "amount", measures, List.of(), List.of(),
                null, null, null);
    }

    private static ExactCubeFragment fragment(String id, long start, long end, List<String> grain,
                                               Set<CubeMeasure> measures, int bytes) {
        return new ExactCubeFragment(id, POPULATION, new CubeInterval(start, end), grain, measures, 1, bytes, bytes, "");
    }
}
