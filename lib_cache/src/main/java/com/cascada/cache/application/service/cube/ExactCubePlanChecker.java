package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeKeyFilterOperator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

final class ExactCubePlanChecker {
    private ExactCubePlanChecker() {}

    static boolean compatible(ExactCubeQuery query, CubePopulation population, ExactCubeFragment fragment, long width) {
        return (fragment.population().equals(population) || narrowed(query, population, fragment))
                && !fragment.measures().isEmpty()
                && query.interval().contains(fragment.interval()) && aligned(fragment.interval(), width)
                && fragment.grain().containsAll(query.grain())
                && new HashSet<>(fragment.grain()).size() == fragment.grain().size()
                && fragment.grain().stream().allMatch(key -> validKeyType(population.source().types().get(key)))
                && fragment.rows() >= 0
                && fragment.compressedBytes() > 0
                && fragment.decodedBytes() > 0;
    }

    static boolean mayNarrow(ExactCubeQuery query, CubePopulation population) {
        return !query.keyFilters().isEmpty() && query.keyFilters().stream().allMatch(filter -> {
            String type = population.source().types().get(filter.column());
            return filter.operator() == CubeKeyFilterOperator.STRING_EQUALS
                    ? "string".equals(type) : validKeyType(type);
        });
    }

    private static boolean narrowed(ExactCubeQuery query, CubePopulation population, ExactCubeFragment fragment) {
        return mayNarrow(query, population) && fragment.population().equals(unfiltered(population))
                && fragment.grain().containsAll(query.predicateTypes().keySet());
    }

    static CubePopulation unfiltered(CubePopulation population) {
        return new CubePopulation(population.source(), population.timeColumn(), "", population.input());
    }

    static boolean aligned(CubeInterval interval, long width) {
        return interval.start() % width == 0 && interval.end() % width == 0;
    }

    static boolean validKeyType(String type) {
        return type != null && Set.of("string", "bigint", "int", "smallint", "tinyint").contains(type);
    }

    static boolean valid(ExactCubeQuery query, CubePopulation population, ExactCubePlan plan, long width) {
        if (!query.measures().equals(plan.covers().keySet()) || !aligned(query.interval(), width)) return false;
        var reads = new HashMap<String, ExactCubeFragment>();
        for (var entry : plan.covers().entrySet()) {
            long cursor = query.interval().start();
            for (var edge : entry.getValue()) {
                if (edge.interval().start() != cursor || !query.interval().contains(edge.interval())
                        || !aligned(edge.interval(), width)) return false;
                cursor = edge.interval().end();
                var fragment = edge.fragment();
                if (fragment == null) continue;
                if (!compatible(query, population, fragment, width) || !fragment.interval().equals(edge.interval())
                        || !fragment.measures().contains(entry.getKey())) return false;
                var previous = reads.putIfAbsent(fragment.generation(), fragment);
                if (previous != null && !previous.equals(fragment)) return false;
            }
            if (cursor != query.interval().end()) return false;
        }
        return !reads.isEmpty();
    }
}
