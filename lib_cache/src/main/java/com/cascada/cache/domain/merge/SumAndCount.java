package com.cascada.cache.domain.merge;

public record SumAndCount(double sum, long count) {
    public SumAndCount {
        if (count < 0) {
            throw new IllegalArgumentException("count must be non-negative, but was: " + count);
        }
    }
}
