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

    Utf8EncodingCache(int rows) { sampleSize = rows >= 4_096 ? 256 : Math.max(1, Math.min(64, rows)); }

    byte[] encode(String value) {
        if (disabled) return value.getBytes(StandardCharsets.UTF_8);
        reads++;
        byte[] encoded = values.get(value);
        if (encoded != null) { hits++; return encoded; }
        encoded = value.getBytes(StandardCharsets.UTF_8);
        if (reads >= sampleSize && (long) hits * 10 < reads) {
            values = null;
            disabled = true;
        } else if (values.size() < 16_384 && encoded.length <= 1_048_576 - bytes) {
            values.put(value, encoded);
            bytes += encoded.length;
        }
        return encoded;
    }
}
