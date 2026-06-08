package com.cascada.cache.domain.port;

import com.cascada.cache.domain.frame.ResultFrame;

/**
 * The seam to distributed execution: the engine hands a (gap-rewritten or full) physical SQL string
 * to this port and receives a {@link ResultFrame}. In production it is the Spark + Gluten/Velox
 * adapter; in tests it is a Tablesaw/in-process {@code DirectComputeOracle}.
 *
 * <p>This is the single boundary that lets the cache's correctness be proven against a simulated
 * executor (mirroring the Python {@code in_memory_simulation.py}) rather than a real cluster
 * (ARCHITECTURE §2).
 */
public interface SparkQueryExecutorPort {

    ResultFrame execute(String physicalSql);
}
