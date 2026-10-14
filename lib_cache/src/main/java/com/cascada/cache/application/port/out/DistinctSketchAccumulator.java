package com.cascada.cache.application.port.out;

/** One group's running union of encoded distinct-count sketches. */
public interface DistinctSketchAccumulator {

    /** Adds one encoded sketch exactly as the executor returned it. */
    void add(String encodedSketch);

    /** The rounded distinct-count estimate of every sketch added so far; 0 when none were added. */
    long estimate();
}
