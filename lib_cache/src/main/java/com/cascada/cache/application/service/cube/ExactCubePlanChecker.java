package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.ExactCubeLimits;
import com.cascada.cache.domain.model.cube.CubeInterval;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

final class ExactCubePlanChecker {
    private ExactCubePlanChecker() {}

    static boolean compatible(ExactCubeQuery query, CubePopulation population, ExactCubeFragment fragment, long width) {
        return fragment.population().equals(population) && !fragment.measures().isEmpty()
                && query.interval().contains(fragment.interval()) && aligned(fragment.interval(), width)
                && fragment.grain().containsAll(query.grain())
                && fragment.grain().size() <= 4 && new HashSet<>(fragment.grain()).size() == fragment.grain().size()
                && fragment.grain().stream().allMatch(key -> validKeyType(population.source().types().get(key)))
                && fragment.rows() >= 0
                && fragment.compressedBytes() > 0 && fragment.compressedBytes() <= ExactCubeLimits.MAX_STATE_BYTES
                && fragment.decodedBytes() > 0 && fragment.decodedBytes() <= ExactCubeLimits.MAX_STATE_BYTES;
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
        int residuals = 0;
        for (var entry : plan.covers().entrySet()) {
            long cursor = query.interval().start();
            for (var edge : entry.getValue()) {
                if (edge.interval().start() != cursor || !query.interval().contains(edge.interval())
                        || !aligned(edge.interval(), width)) return false;
                cursor = edge.interval().end();
                var fragment = edge.fragment();
                if (fragment == null) {
                    residuals++;
                    continue;
                }
                if (!compatible(query, population, fragment, width) || !fragment.interval().equals(edge.interval())
                        || !fragment.measures().contains(entry.getKey())) return false;
                var previous = reads.putIfAbsent(fragment.generation(), fragment);
                if (previous != null && !previous.equals(fragment)) return false;
            }
            if (cursor != query.interval().end()) return false;
        }
        return !reads.isEmpty() && residuals <= 8 && reads.size() <= 16
                && reads.values().stream().mapToLong(ExactCubeFragment::decodedBytes).sum() <= ExactCubeLimits.MAX_STATE_BYTES
                && reads.values().stream().mapToLong(ExactCubeFragment::compressedBytes).sum() <= ExactCubeLimits.MAX_STATE_BYTES;
    }
}
