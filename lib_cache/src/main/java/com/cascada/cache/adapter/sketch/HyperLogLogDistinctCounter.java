package com.cascada.cache.adapter.sketch;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;

import java.util.List;

/**
 * A mergeable approximate distinct-count, wrapping Apache DataSketches HLL (plan §8.13). This is what
 * lets {@code COUNT(DISTINCT col)} become cacheable: each day-bucket stores an HLL sketch, and a
 * multi-day distinct count is the HLL <em>union</em> of the per-day sketches (≈1–2% error) — something
 * exact counts can never do across buckets, which is why {@code ImpossibleMathRule} bypasses the exact
 * form.
 *
 * <p>Sketches serialize to bytes so they ride inside the same cache blob as the SUM/COUNT ingredients.
 */
public final class HyperLogLogDistinctCounter {

    private static final int DEFAULT_LOG_K = 12; // ~1.6% relative standard error

    private final HllSketch sketch;

    public HyperLogLogDistinctCounter() {
        this.sketch = new HllSketch(DEFAULT_LOG_K, TgtHllType.HLL_8);
    }

    private HyperLogLogDistinctCounter(HllSketch sketch) {
        this.sketch = sketch;
    }

    public void add(String value) {
        sketch.update(value);
    }

    public void add(long value) {
        sketch.update(value);
    }

    public double estimateDistinctCount() {
        return sketch.getEstimate();
    }

    // Blob round-trip (toCompactByteArray + heapify) is deferred: datasketches-memory 3.0.2 refuses
    // JDK > 21, and this repo builds on JDK 22 (release 21). Live merge below is JDK-independent.

    /** Union per-day sketches into one multi-day distinct-count estimate (the cross-bucket merge). */
    public static HyperLogLogDistinctCounter unionOf(List<HyperLogLogDistinctCounter> counters) {
        Union union = new Union(DEFAULT_LOG_K);
        for (HyperLogLogDistinctCounter counter : counters) {
            union.update(counter.sketch);
        }
        return new HyperLogLogDistinctCounter(union.getResult(TgtHllType.HLL_8));
    }
}
