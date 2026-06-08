package com.cascada.cache.domain.merge;

/**
 * The distributive/algebraic aggregate functions the cache can roll up across buckets, with the
 * crucial detail (ported from the {@code agg_map} in {@code merging.py}) that {@code COUNT} combines
 * by <em>summing</em> partial counts — never by counting again.
 *
 * <p>Holistic aggregates ({@code COUNT(DISTINCT)}, exact {@code MEDIAN}) are deliberately absent:
 * they cannot be combined exactly and are handled by the bypass rule or the sketch path (plan §8.13).
 */
public enum AggregateFunction {

    SUM {
        @Override
        public double combine(double left, double right) {
            return left + right;
        }
    },

    /** Partial counts merge by addition — this is why the engine stores SUM and COUNT, not AVG. */
    COUNT {
        @Override
        public double combine(double left, double right) {
            return left + right;
        }
    },

    MINIMUM {
        @Override
        public double combine(double left, double right) {
            return Math.min(left, right);
        }
    },

    MAXIMUM {
        @Override
        public double combine(double left, double right) {
            return Math.max(left, right);
        }
    };

    public abstract double combine(double left, double right);
}
