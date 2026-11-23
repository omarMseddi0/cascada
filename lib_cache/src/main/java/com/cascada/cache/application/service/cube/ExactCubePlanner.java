package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.model.query.CubeCoverEdge;
import com.cascada.cache.application.model.query.CubeCoverPosition;
import com.cascada.cache.application.model.query.CubeCoverStep;
import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;

/** Finds the cheapest cover, pricing a shared read or scan once when the two covers advance together. */
public final class ExactCubePlanner {
    private ExactCubePlanner() {}

    public static Optional<ExactCubePlan> select(ExactCubeQuery query, CubePopulation population,
                                                 List<ExactCubeFragment> candidates, long bucketSeconds) {
        var ingredients = query.measures().stream().sorted().toList();
        if (ingredients.isEmpty()) return Optional.empty();
        long end = query.interval().end();
        var starts = new TreeSet<Long>();
        starts.add(end);

        // Equal measure sets over the same interval can use the cheapest verified fragment.
        // Keep distinct measure sets so a shared read is still available to the optimizer.
        var byStart = new HashMap<Long, Map<CubeInterval, Map<Set<CubeMeasure>, ExactCubeFragment>>>();
        for (var fragment : candidates) {
            QueryCancellation.requireCurrentActive();
            if (!ExactCubePlanChecker.compatible(query, population, fragment, bucketSeconds)
                    || Collections.disjoint(fragment.measures(), query.measures())) continue;
            var interval = fragment.interval();
            starts.add(interval.start());
            starts.add(interval.end());
            byStart.computeIfAbsent(interval.start(), ignored -> new HashMap<>())
                    .computeIfAbsent(interval, ignored -> new HashMap<>())
                    .merge(fragment.measures(), fragment,
                            (a, b) -> a.decodedBytes() <= b.decodedBytes() ? a : b);
        }
        if (byStart.isEmpty()) return Optional.empty();

        boolean joint = ingredients.size() == 2;
        long start = query.interval().start();
        var initial = new CubeCoverStep(new CubeCoverPosition(start, joint ? start : end), null, null, 0);
        var best = new HashMap<CubeCoverPosition, CubeCoverStep>();
        var queue = new PriorityQueue<>(Comparator.comparingDouble(CubeCoverStep::cost));
        best.put(initial.position(), initial);
        queue.add(initial);
        double directCost = scanCost(query.interval(), bucketSeconds);
        while (!queue.isEmpty()) {
            QueryCancellation.requireCurrentActive();
            var step = queue.remove();
            var position = step.position();
            if (best.get(position) != step) continue;
            long first = position.first(), second = position.second();
            if (first == end && second == end) {
                var plan = materialize(step, ingredients);
                return ExactCubePlanChecker.valid(query, population, plan, bucketSeconds)
                        ? Optional.of(plan) : Optional.empty();
            }
            long at = Math.min(first, second);
            for (var group : byStart.getOrDefault(at, Map.of()).values()) {
                for (var fragment : group.values()) {
                    advance(step, new CubeCoverEdge(fragment.interval(), fragment), ingredients,
                            0.02 + fragment.decodedBytes() * (0.125 / (1024 * 1024)), directCost, best, queue);
                }
            }
            for (long stop : starts.tailSet(at, false)) {
                var interval = new CubeInterval(at, stop);
                double cost = scanCost(interval, bucketSeconds);
                if (step.cost() + cost >= directCost) break;
                advance(step, new CubeCoverEdge(interval, null), ingredients, cost, directCost, best, queue);
            }
        }
        return Optional.empty();
    }

    private static void advance(CubeCoverStep step, CubeCoverEdge edge, List<CubeMeasure> ingredients,
                                double edgeCost, double directCost, Map<CubeCoverPosition, CubeCoverStep> best,
                                PriorityQueue<CubeCoverStep> queue) {
        double cost = step.cost() + edgeCost;
        if (cost >= directCost) return;
        var position = step.position();
        long start = edge.interval().start(), end = edge.interval().end();
        var fragment = edge.fragment();
        boolean first = position.first() == start
                && (fragment == null || fragment.measures().contains(ingredients.getFirst()));
        boolean second = ingredients.size() == 2 && position.second() == start
                && (fragment == null || fragment.measures().contains(ingredients.getLast()));

        // A shared edge can occur only when both paths are at its start. Advancing
        // the earlier path first preserves the option to share at the next meeting point.
        if (first) offer(new CubeCoverPosition(end, position.second()), step, edge, cost, best, queue);
        if (second) offer(new CubeCoverPosition(position.first(), end), step, edge, cost, best, queue);
        if (first && second) offer(new CubeCoverPosition(end, end), step, edge, cost, best, queue);
    }

    private static void offer(CubeCoverPosition position, CubeCoverStep step, CubeCoverEdge edge, double cost,
                              Map<CubeCoverPosition, CubeCoverStep> best, PriorityQueue<CubeCoverStep> queue) {
        var incumbent = best.get(position);
        if (incumbent != null && incumbent.cost() <= cost) return;
        var next = new CubeCoverStep(position, step, edge, cost);
        best.put(position, next);
        queue.add(next);
    }

    private static ExactCubePlan materialize(CubeCoverStep end, List<CubeMeasure> ingredients) {
        var first = new ArrayList<CubeCoverEdge>();
        var second = new ArrayList<CubeCoverEdge>();
        for (var step = end; step.previous() != null; step = step.previous()) {
            var previous = step.previous().position();
            if (step.position().first() != previous.first()) first.add(step.edge());
            if (step.position().second() != previous.second()) second.add(step.edge());
        }
        var covers = new EnumMap<CubeMeasure, List<CubeCoverEdge>>(CubeMeasure.class);
        covers.put(ingredients.getFirst(), List.copyOf(first.reversed()));
        if (ingredients.size() == 2) covers.put(ingredients.getLast(), List.copyOf(second.reversed()));
        return new ExactCubePlan(Map.copyOf(covers));
    }

    private static double scanCost(CubeInterval interval, long bucketSeconds) {
        return 1 + (double) (interval.end() - interval.start()) / bucketSeconds;
    }
}
