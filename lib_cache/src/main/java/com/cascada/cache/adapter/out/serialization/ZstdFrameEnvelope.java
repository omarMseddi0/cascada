package com.cascada.cache.adapter.out.serialization;

import com.github.luben.zstd.Zstd;
import org.agrona.concurrent.UnsafeBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/** Shared, wire-compatible [big-endian length][zstd frame] envelope. */
final class ZstdFrameEnvelope {
    private ZstdFrameEnvelope() { }

    static byte[] compress(byte[] source, int length, int level) {
        int capacity = Math.toIntExact(Math.addExact(4L, Zstd.compressBound(length)));
        byte[] blob = new byte[capacity];
        new UnsafeBuffer(blob).putInt(0, length, ByteOrder.BIG_ENDIAN);
        long written = Zstd.compressByteArray(blob, 4, capacity - 4, source, 0, length, level);
        if (Zstd.isError(written)) throw new IllegalArgumentException(Zstd.getErrorName(written));
        return Arrays.copyOf(blob, Math.toIntExact(written + 4));
    }

    static byte[] decompress(byte[] blob) {
        if (blob == null || blob.length < 4) throw new IllegalArgumentException("truncated cache envelope");
        int length = new UnsafeBuffer(blob).getInt(0, ByteOrder.BIG_ENDIAN);
        if (length < 0 || Zstd.decompressedSize(blob, 4, blob.length - 4) != length) {
            throw new IllegalArgumentException("invalid uncompressed frame length");
        }
        byte[] decoded = new byte[length];
        long written = Zstd.decompressByteArray(decoded, 0, length, blob, 4, blob.length - 4);
        if (Zstd.isError(written) || written != length) {
            throw new IllegalArgumentException("invalid compressed frame data: " + Zstd.getErrorName(written));
        }
        return decoded;
    }
}
