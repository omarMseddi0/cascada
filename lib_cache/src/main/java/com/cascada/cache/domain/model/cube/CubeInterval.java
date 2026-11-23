package com.cascada.cache.domain.model.cube;

public record CubeInterval(long start, long end) {
    public CubeInterval {
        if (start >= end) throw new IllegalArgumentException("empty or reversed interval");
        Math.subtractExact(end, start);
    }

    public boolean contains(CubeInterval other) {
        return start <= other.start && end >= other.end;
    }
}
