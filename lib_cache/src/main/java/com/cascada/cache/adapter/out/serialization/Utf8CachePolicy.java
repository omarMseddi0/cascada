package com.cascada.cache.adapter.out.serialization;

/**
 * Admission policy shared by the UTF-8 encode and decode caches: sample the first values of a column,
 * give up when fewer than one in ten repeat, and bound what a column may retain.
 */
final class Utf8CachePolicy {

    static final int MAX_ENTRIES = 16_384;
    static final int MAX_BYTES = 1_048_576;

    private Utf8CachePolicy() {
    }

    /** Values sampled before deciding whether a column repeats enough to be worth caching. */
    static int sampleSize(int rows) {
        return rows >= 4_096 ? 256 : Math.max(1, Math.min(64, rows));
    }

    /** A sampled column with fewer than 10% repeated values is effectively distinct: stop caching it. */
    static boolean mostlyDistinct(int reads, int hits, int sampleSize) {
        return reads >= sampleSize && (long) hits * 10 < reads;
    }

    static boolean admits(int entries, int retainedBytes, int valueBytes) {
        return entries < MAX_ENTRIES && valueBytes <= MAX_BYTES - retainedBytes;
    }
}
