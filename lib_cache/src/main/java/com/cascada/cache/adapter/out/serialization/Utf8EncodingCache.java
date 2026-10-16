package com.cascada.cache.adapter.out.serialization;

import java.nio.charset.StandardCharsets;
import org.agrona.collections.Object2ObjectHashMap;

/** Bounded per-column UTF-8 reuse; columns with mostly distinct values disable the cache early. */
final class Utf8EncodingCache {
    private Object2ObjectHashMap<String, byte[]> values = new Object2ObjectHashMap<>();
    private int reads;
    private int hits;
    private int bytes;
    private boolean disabled;
    private final int sampleSize;

    Utf8EncodingCache(int rows) { sampleSize = Utf8CachePolicy.sampleSize(rows); }

    byte[] encode(String value) {
        if (disabled) return value.getBytes(StandardCharsets.UTF_8);
        reads++;
        byte[] encoded = values.get(value);
        if (encoded != null) { hits++; return encoded; }
        encoded = value.getBytes(StandardCharsets.UTF_8);
        if (Utf8CachePolicy.mostlyDistinct(reads, hits, sampleSize)) {
            values = null;
            disabled = true;
        } else if (Utf8CachePolicy.admits(values.size(), bytes, encoded.length)) {
            values.put(value, encoded);
            bytes += encoded.length;
        }
        return encoded;
    }
}
