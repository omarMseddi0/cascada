package com.cascada.cache.adapter.out.serialization;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.VarCharVector;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/** Per-decode, bounded dictionary: repeated UTF-8 cells need no new byte array or String. */
final class Utf8ValueCache {
    private static final VarHandle NATIVE_LONG =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final int MAX_ENTRIES = 16_384;
    private static final int MAX_KEY_BYTES = 1_048_576;
    private final int sampleSize;

    private byte[][] keys;
    private String[] values;
    private int[] hashes;
    private int count;
    private int keyBytes;
    private int reads;
    private int hits;
    private boolean disabled;

    Utf8ValueCache() { this(4_096); }
    Utf8ValueCache(int sampleSize) { this.sampleSize = Math.max(1, sampleSize); }

    String read(VarCharVector vector, int row) {
        if (disabled) return decode(vector.get(row));
        return readCached(vector, row);
    }

    private String readCached(VarCharVector vector, int row) {
        if (keys == null) {
            keys = new byte[256][];
            values = new String[256];
            hashes = new int[256];
        }
        ArrowBuf offsets = vector.getOffsetBuffer();
        int start = offsets.getInt((long) row * Integer.BYTES);
        int end = offsets.getInt((long) (row + 1) * Integer.BYTES);
        int length = Math.subtractExact(end, start);
        if (length < 0) throw new IllegalArgumentException("invalid UTF-8 cell offsets");
        if (length > MAX_KEY_BYTES) return decode(vector.get(row));
        ArrowBuf data = vector.getDataBuffer();
        int hash = hash(data, start, length);
        int slot = hash & (keys.length - 1);
        while (keys[slot] != null) {
            if (hashes[slot] == hash && equal(data, start, length, keys[slot])) {
                hits++;
                reads++;
                return values[slot];
            }
            slot = (slot + 1) & (keys.length - 1);
        }
        reads++;
        byte[] bytes = vector.get(row);
        String result = decode(bytes);
        // Stop spending hash-table CPU/storage on a predominantly distinct column.
        if (reads >= sampleSize && (long) hits * 10 < reads) {
            disabled = true;
            keys = null;
            values = null;
            hashes = null;
            return result;
        }
        if (count >= MAX_ENTRIES || length > MAX_KEY_BYTES - keyBytes) return result;
        if ((count + 1) * 2 >= keys.length) {
            grow();
            slot = hash & (keys.length - 1);
            while (keys[slot] != null) slot = (slot + 1) & (keys.length - 1);
        }
        keys[slot] = bytes;
        values[slot] = result;
        hashes[slot] = hash;
        keyBytes += length;
        count++;
        return result;
    }

    private void grow() {
        byte[][] oldKeys = keys;
        String[] oldValues = values;
        int[] oldHashes = hashes;
        keys = new byte[oldKeys.length * 2][];
        values = new String[keys.length];
        hashes = new int[keys.length];
        for (int index = 0; index < oldKeys.length; index++) {
            if (oldKeys[index] == null) continue;
            int slot = oldHashes[index] & (keys.length - 1);
            while (keys[slot] != null) slot = (slot + 1) & (keys.length - 1);
            keys[slot] = oldKeys[index];
            values[slot] = oldValues[index];
            hashes[slot] = oldHashes[index];
        }
    }

    private static String decode(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int hash(ArrowBuf data, int start, int length) {
        long hash = length ^ 0x9e3779b97f4a7c15L;
        int index = 0;
        for (; index + Long.BYTES <= length; index += Long.BYTES) {
            hash = Long.rotateLeft(hash ^ data.getLong((long) start + index), 27) * 0xc2b2ae3d27d4eb4fL;
        }
        for (; index < length; index++) hash = (hash ^ (data.getByte((long) start + index) & 0xffL)) * 0x100000001b3L;
        hash ^= hash >>> 32;
        return (int) hash;
    }

    private static boolean equal(ArrowBuf data, int start, int length, byte[] bytes) {
        if (bytes.length != length) return false;
        int index = 0;
        for (; index + Long.BYTES <= length; index += Long.BYTES) {
            if (data.getLong((long) start + index) != (long) NATIVE_LONG.get(bytes, index)) return false;
        }
        for (; index < length; index++) if (data.getByte((long) start + index) != bytes[index]) return false;
        return true;
    }
}
