package com.cascada.cache.adapter.out.serialization;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.agrona.concurrent.UnsafeBuffer;

/** Shared, wire-compatible [big-endian length][zstd frame] envelope. */
final class ZstdFrameEnvelope {
    static final int DEFAULT_COMPRESSION_LEVEL = 3;
    static final int HEADER_BYTES = 4;

    private ZstdFrameEnvelope() { }

    /** The level every serializer accepts: Zstandard's own supported range. */
    static int requireCompressionLevel(int level) {
        if (level < Zstd.minCompressionLevel() || level > Zstd.maxCompressionLevel()) {
            throw new IllegalArgumentException("compressionLevel is outside the supported Zstandard range");
        }
        return level;
    }

    /** Writes the envelope's big-endian uncompressed-length header. */
    static void writeHeader(java.io.ByteArrayOutputStream out, int uncompressedLength) {
        byte[] header = new byte[HEADER_BYTES];
        new UnsafeBuffer(header).putInt(0, uncompressedLength, ByteOrder.BIG_ENDIAN);
        out.write(header, 0, HEADER_BYTES);
    }

    static byte[] compress(byte[] source, int length, int level) {
        int capacity = Math.toIntExact(Math.addExact((long) HEADER_BYTES, Zstd.compressBound(length)));
        byte[] blob = new byte[capacity];
        new UnsafeBuffer(blob).putInt(0, length, ByteOrder.BIG_ENDIAN);
        try (ZstdCompressCtx compressor = new ZstdCompressCtx()) {
            compressor.setLevel(level).setChecksum(true).setContentSize(true);
            int written = compressor.compressByteArray(blob, HEADER_BYTES, capacity - HEADER_BYTES, source, 0, length);
            return Arrays.copyOf(blob, Math.addExact(written, HEADER_BYTES));
        }
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
