package com.cascada.cache.adapter.out.serialization;

import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;
import java.nio.charset.StandardCharsets;

/** Per-decode bounded dictionary, using Agrona's byte comparison, hashing and open-addressed map. */
final class Utf8ValueCache {
    private static final int MAX_ENTRIES = 16_384;
    private static final int MAX_KEY_BYTES = 1_048_576;
    private final int sampleSize;
    // Lookup view is never inserted. Stored keys own their bytes, surviving Arrow buffer reuse.
    private final UnsafeBuffer lookup = new UnsafeBuffer(new byte[0]);
    private Object2ObjectHashMap<UnsafeBuffer, String> values;
    private int keyBytes;
    private int reads;
    private int hits;
    private boolean disabled;

    Utf8ValueCache() { this(256); }
    Utf8ValueCache(int sampleSize) { this.sampleSize = Math.max(1, sampleSize); }

    String read(VarCharVector vector, int row) {
        if (disabled) return decode(vector.get(row));
        ArrowBuf offsets = vector.getOffsetBuffer();
        int start = offsets.getInt((long) row * Integer.BYTES);
        int end = offsets.getInt((long) (row + 1) * Integer.BYTES);
        int length = Math.subtractExact(end, start);
        ArrowBuf data = vector.getDataBuffer();
        if (start < 0 || length < 0 || end > data.capacity()) {
            throw new IllegalArgumentException("invalid UTF-8 cell offsets");
        }
        if (length > MAX_KEY_BYTES) return decode(vector.get(row));
        if (values == null) values = new Object2ObjectHashMap<>(256, 0.5f);
        // Arrow owns this memory throughout read(); never retain its address between calls.
        lookup.wrap(data.memoryAddress() + start, length);
        try {
            String existing = values.get(lookup);
            reads++;
            if (existing != null) {
                hits++;
                return existing;
            }
            byte[] bytes = vector.get(row);
            String result = decode(bytes);
            if (reads >= sampleSize && (long) hits * 10 < reads) {
                disabled = true;
                values = null;
                return result;
            }
            if (values.size() < MAX_ENTRIES && length <= MAX_KEY_BYTES - keyBytes) {
                values.put(new UnsafeBuffer(bytes), result);
                keyBytes += length;
            }
            return result;
        } finally {
            // No dangling native pointer after Arrow releases or reallocates its data buffer.
            lookup.wrap(0L, 0);
        }
    }

    private static String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
