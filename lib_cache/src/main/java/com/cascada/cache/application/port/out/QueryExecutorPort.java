package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;
import java.util.List;
import java.util.Map;

/**
 * The seam to distributed execution: the engine hands a (gap-rewritten or full) physical SQL string
 * to this port and receives a {@link ResultFrame}. In production it is the Spark + Gluten/Velox
 * adapter; in tests it is a Tablesaw/in-process {@code DirectComputeOracle}.
 *
 * <p>This is the single boundary that lets the cache's correctness be proven against a simulated
 * executor (mirroring the Python {@code in_memory_simulation.py}) rather than a real cluster
 * (ARCHITECTURE §2).
 */
public interface QueryExecutorPort {

    ResultFrame execute(String physicalSql);

    /**
     * Evaluates SQL expressions over an already merged, driver-side frame: {@code projections} are
     * select-list items in the executor's dialect, and {@code declaredTypes} gives the exact SQL type
     * of input columns whose frame type is not precise enough (for example a DECIMAL's precision).
     * Used to finish expressions over aggregates with the engine's own arithmetic semantics.
     */
    default ResultFrame project(ResultFrame input, Map<String, String> declaredTypes, List<String> projections) {
        throw new UnsupportedOperationException("this executor cannot evaluate expressions over a merged frame");
    }
}
