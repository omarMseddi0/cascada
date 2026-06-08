package com.cascada.sparkconfig.domain;

/**
 * How one executor's RAM budget is divided between JVM heap, Gluten/Velox off-heap, and overhead.
 *
 * <p>The invariant {@code heap + offHeap + overhead <= totalRam} is what stops a misconfiguration
 * from OOM-ing in production (plan §5.1, §6.6) — it is asserted as a property over a wide knob
 * matrix. When the workload does not use Gluten off-heap (the {@code MIXED} reference), the whole
 * budget is heap, matching the golden config's single {@code spark.executor.memory} value.
 */
public record SparkMemorySplit(int heapGigabytes, int offHeapGigabytes, int overheadGigabytes) {

    public SparkMemorySplit {
        if (heapGigabytes <= 0) {
            throw new IllegalArgumentException("heap must be > 0, but was: " + heapGigabytes);
        }
        if (offHeapGigabytes < 0 || overheadGigabytes < 0) {
            throw new IllegalArgumentException("off-heap and overhead must be non-negative");
        }
    }

    public int totalGigabytes() {
        return heapGigabytes + offHeapGigabytes + overheadGigabytes;
    }

    public String heapAsSparkMemoryString() {
        return heapGigabytes + "g";
    }
}
