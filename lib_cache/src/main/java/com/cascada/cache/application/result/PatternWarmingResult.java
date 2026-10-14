package com.cascada.cache.application.result;

public record PatternWarmingResult(int bucketsWarmed, int bucketsSkipped) {
    public PatternWarmingResult {
        if (bucketsWarmed < 0 || bucketsSkipped < 0) {
            throw new IllegalArgumentException("bucket counts must be non-negative");
        }
    }
}
