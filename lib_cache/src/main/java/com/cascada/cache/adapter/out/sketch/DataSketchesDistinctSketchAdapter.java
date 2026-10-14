package com.cascada.cache.adapter.out.sketch;

import com.cascada.cache.application.port.out.DistinctSketchAccumulator;
import com.cascada.cache.application.port.out.DistinctSketchPort;
import com.cascada.cache.domain.merge.DistinctSketchPrecision;

/**
 * Unions the Apache DataSketches HLL sketches Spark produces with {@code hll_sketch_agg}. Spark 4.1
 * ships the same datasketches-java 6.2.0 used here, so the bytes are read without conversion.
 */
public final class DataSketchesDistinctSketchAdapter implements DistinctSketchPort {

    /** The lgConfigK the SQL rewriter passes to {@code hll_sketch_agg}. */
    public static final int LOG_CONFIG_K = DistinctSketchPrecision.LOG_CONFIG_K;

    @Override
    public DistinctSketchAccumulator newAccumulator() {
        return new HllSketchUnion(LOG_CONFIG_K);
    }
}
