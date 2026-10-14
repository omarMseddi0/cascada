package com.cascada.cache.domain.merge;

/** Aggregate identity; partial COUNT values merge by addition in the application layer. */
public enum AggregateFunction {
    SUM, COUNT, MINIMUM, MAXIMUM
}
