package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.frame.ResultFrame;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Per-scan filter predicates, evaluated in the input set's iteration order. */
final class CompiledFilters {
    private final List<CompiledFilter> predicates;

    CompiledFilters(CubeFilterEvaluator evaluator, Set<String> filters) {
        List<CompiledFilter> compiled = new ArrayList<>(filters.size());
        for (String filter : filters) {
            compiled.add(new CompiledFilter(evaluator, filter));
        }
        this.predicates = List.copyOf(compiled);
    }

    boolean matches(ResultFrame frame, int rowIndex) {
        for (CompiledFilter filter : predicates) {
            if (!filter.matches(frame, rowIndex)) {
                return false;
            }
        }
        return true;
    }
}
