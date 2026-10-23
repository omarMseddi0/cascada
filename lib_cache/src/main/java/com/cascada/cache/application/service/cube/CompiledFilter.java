package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.model.cube.CubeFilterLiteral;
import com.cascada.cache.application.model.cube.PreparedCubeFilter;
import com.cascada.cache.domain.frame.ResultFrame;

/** One lazily prepared filter predicate, preserving row-by-row short-circuit behavior. */
final class CompiledFilter {
    private final CubeFilterEvaluator evaluator;
    private final String source;
    private PreparedCubeFilter prepared;

    CompiledFilter(CubeFilterEvaluator evaluator, String source) {
        this.evaluator = evaluator;
        this.source = source;
    }

    boolean matches(ResultFrame frame, int rowIndex) {
        if (prepared == null) {
            prepared = evaluator.prepare(frame, source);
        }
        if (prepared.inFilter()) {
            for (CubeFilterLiteral literal : prepared.literals()) {
                if (evaluator.frameValueEqualsLiteral(frame, rowIndex, prepared.columnIndex(),
                        prepared.type(), literal)) {
                    return true;
                }
            }
            return false;
        }
        return evaluator.frameValueEqualsLiteral(frame, rowIndex, prepared.columnIndex(), prepared.type(),
                prepared.literals().get(0));
    }
}
