package com.cascada.sparkconfig.domain;

/**
 * The optional workload hint (plan §6.6 tier 2) that nudges the derivation without exposing any
 * raw Spark flag. It decides, among other things, whether the Gluten/Velox off-heap split is
 * applied to the memory budget.
 *
 * <p>{@link #MIXED} is the reference default and keeps the whole RAM budget on the JVM heap, which
 * is what the golden {@code spark.json} encodes (no {@code spark.memory.offHeap.size} key).
 */
public enum WorkloadType {

    INTERACTIVE_DASHBOARD(true),
    BATCH_TRANSFORM(true),
    TIME_SERIES(true),
    MIXED(false);

    private final boolean glutenOffHeapEnabled;

    WorkloadType(boolean glutenOffHeapEnabled) {
        this.glutenOffHeapEnabled = glutenOffHeapEnabled;
    }

    public boolean isGlutenOffHeapEnabled() {
        return glutenOffHeapEnabled;
    }
}
