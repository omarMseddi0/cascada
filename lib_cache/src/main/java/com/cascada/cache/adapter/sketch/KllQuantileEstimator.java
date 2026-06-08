package com.cascada.cache.adapter.sketch;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

import java.util.List;

/**
 * A mergeable approximate quantile estimator, wrapping Apache DataSketches KLL (plan §8.13). This is
 * what makes {@code PERCENTILE}/{@code MEDIAN} cacheable: each day-bucket stores a KLL sketch, and a
 * multi-day quantile is the KLL <em>merge</em> of the per-day sketches (within a bounded rank error) —
 * impossible for exact quantiles across buckets.
 */
public final class KllQuantileEstimator {

    private static final int DEFAULT_K = 200; // controls accuracy/size tradeoff

    private final KllDoublesSketch sketch;

    public KllQuantileEstimator() {
        this.sketch = KllDoublesSketch.newHeapInstance(DEFAULT_K);
    }

    private KllQuantileEstimator(KllDoublesSketch sketch) {
        this.sketch = sketch;
    }

    public void add(double value) {
        sketch.update(value);
    }

    /** The value at the given rank in {@code [0,1]} (e.g. {@code 0.95} for p95). */
    public double quantile(double rank) {
        return sketch.getQuantile(rank, QuantileSearchCriteria.INCLUSIVE);
    }

    // Blob round-trip deferred (datasketches-memory 3.0.2 refuses JDK > 21); live merge is JDK-independent.

    /** Merge per-day sketches into one multi-day quantile estimator (the cross-bucket merge). */
    public static KllQuantileEstimator mergeOf(List<KllQuantileEstimator> estimators) {
        KllDoublesSketch merged = KllDoublesSketch.newHeapInstance(DEFAULT_K);
        for (KllQuantileEstimator estimator : estimators) {
            merged.merge(estimator.sketch);
        }
        return new KllQuantileEstimator(merged);
    }
}
