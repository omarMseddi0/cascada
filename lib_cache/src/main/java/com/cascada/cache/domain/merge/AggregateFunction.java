package com.cascada.cache.domain.merge;

/**
 * Aggregate identity. Partial COUNT values merge by addition in the application layer; AVERAGE is
 * stored as SUM, COUNT and input-type ingredients and rebuilt with Spark's result rules.
 */
public enum AggregateFunction {
    SUM, COUNT, MINIMUM, MAXIMUM, AVERAGE
}
