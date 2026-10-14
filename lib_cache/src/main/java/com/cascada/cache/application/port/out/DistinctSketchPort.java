package com.cascada.cache.application.port.out;

/**
 * Merges the serialized distinct-count sketches stored for {@code APPROX_COUNT_DISTINCT} measures.
 * The sketch library stays behind this port so the merge algorithm remains framework-free.
 */
public interface DistinctSketchPort {

    /** A fresh, empty union for one output group. */
    DistinctSketchAccumulator newAccumulator();
}
