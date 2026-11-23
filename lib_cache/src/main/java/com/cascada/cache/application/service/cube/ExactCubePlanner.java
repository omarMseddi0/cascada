package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.application.model.query.CubeCoverEdge;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubeInterval;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Bounded forward graph. Keep alternative covers for joint pricing. */
public final class ExactCubePlanner {
    static final int CANDIDATES = 64;
    static final int MAX_BYTES = 8 * 1024 * 1024;
    static final long MAX_PLANNING_NANOS = 25_000_000L;
    private static final int ALTERNATIVES = 8;

    private ExactCubePlanner() {}

    public static Optional<ExactCubePlan> select(ExactCubeQuery query, CubePopulation population,
                                                 List<ExactCubeFragment> candidates, long bucketSeconds) {
        if (candidates.size() > CANDIDATES) return Optional.empty();
        long started = System.nanoTime();
        var eligible = candidates.stream().filter(f -> ExactCubePlanChecker.compatible(
                query, population, f, bucketSeconds)).distinct().toList();
        var ingredients = query.measures().stream().sorted().toList();
        if (eligible.isEmpty() || ingredients.isEmpty()) return Optional.empty();
        var first = paths(query.interval(), ingredients.getFirst(), eligible, started);
        var second = ingredients.size() == 1 ? List.<List<CubeCoverEdge>>of(List.of())
                : paths(query.interval(), ingredients.getLast(), eligible, started);
        double cheapest = score(Map.of(ingredients.getFirst(), List.of(new CubeCoverEdge(query.interval(), null))),
                bucketSeconds);
        ExactCubePlan best = null;
        for (List<CubeCoverEdge> left : first) {
            for (List<CubeCoverEdge> right : second) {
                if (timedOut(started)) return Optional.empty();
                Map<CubeMeasure, List<CubeCoverEdge>> covers = new EnumMap<>(CubeMeasure.class);
                covers.put(ingredients.getFirst(), left);
                if (ingredients.size() == 2) covers.put(ingredients.getLast(), right);
                double cost = score(covers, bucketSeconds);
                if (cost < cheapest) {
                    var plan = new ExactCubePlan(Map.copyOf(covers));
                    if (ExactCubePlanChecker.valid(query, population, plan, bucketSeconds)) {
                        best = plan;
                        cheapest = cost;
                    }
                }
            }
        }
        return Optional.ofNullable(best);
    }

    private static List<List<CubeCoverEdge>> paths(CubeInterval window, CubeMeasure measure,
                                          List<ExactCubeFragment> candidates, long started) {
        TreeSet<Long> vertices = new TreeSet<>(Set.of(window.start(), window.end()));
        Map<Long, List<ExactCubeFragment>> byStart = new HashMap<>();
        for (ExactCubeFragment f : candidates) {
            if (!f.measures().contains(measure)) continue;
            vertices.add(f.interval().start());
            vertices.add(f.interval().end());
            byStart.computeIfAbsent(f.interval().start(), ignored -> new ArrayList<>()).add(f);
        }
        var suffixes = new HashMap<Long, List<List<CubeCoverEdge>>>();
        suffixes.put(window.end(), List.of(List.of()));
        for (long start : vertices.descendingSet()) {
            if (start == window.end()) continue;
            if (timedOut(started)) return List.of();
            List<List<CubeCoverEdge>> options = new ArrayList<>();
            for (var fragment : byStart.getOrDefault(start, List.of())) {
                prepend(options, new CubeCoverEdge(fragment.interval(), fragment), suffixes.get(fragment.interval().end()));
            }
            // A residual ends at the next cache contribution or at the end of the window.
            for (long end : vertices.tailSet(start, false)) {
                if (end != window.end() && !byStart.containsKey(end)) continue;
                var tails = suffixes.get(end).stream()
                        .filter(p -> p.isEmpty() || p.getFirst().fragment() != null).toList();
                prepend(options, new CubeCoverEdge(new CubeInterval(start, end), null), tails);
            }
            options.sort(Comparator.comparingDouble(path -> score(Map.of(measure, path), window.end() - window.start())));
            suffixes.put(start, List.copyOf(options.subList(0, Math.min(ALTERNATIVES, options.size()))));
        }
        var result = new ArrayList<>(suffixes.get(window.start()));
        var direct = List.of(new CubeCoverEdge(window, null));
        if (!result.contains(direct)) result.add(direct);
        return result;
    }

    private static void prepend(List<List<CubeCoverEdge>> options, CubeCoverEdge edge, List<List<CubeCoverEdge>> suffixes) {
        for (var suffix : suffixes) {
            List<CubeCoverEdge> path = new ArrayList<>(suffix.size() + 1);
            path.add(edge);
            path.addAll(suffix);
            options.add(path);
        }
    }

    private static boolean timedOut(long started) {
        return System.nanoTime() - started > MAX_PLANNING_NANOS;
    }

    private static double score(Map<CubeMeasure, List<CubeCoverEdge>> covers, long bucketSeconds) {
        Set<String> reads = new HashSet<>();
        Set<CubeInterval> scans = new HashSet<>();
        double cost = 0;
        for (List<CubeCoverEdge> edges : covers.values()) {
            for (CubeCoverEdge edge : edges) {
                if (edge.fragment() == null) {
                    if (scans.add(edge.interval())) {
                        cost += 1 + (double) (edge.interval().end() - edge.interval().start()) / bucketSeconds;
                    }
                } else if (reads.add(edge.fragment().generation())) {
                    cost += 0.02 + edge.fragment().decodedBytes() / (double) MAX_BYTES;
                }
            }
        }
        return cost;
    }
}
