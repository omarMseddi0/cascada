package com.cascada.cache.adapter.out.serialization;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import org.agrona.concurrent.UnsafeBuffer;

/** Logical read bounds are enforced even when Agrona global bounds checking is disabled. */
final class PortableFrameReader {
    private final UnsafeBuffer buffer;
    private final ByteBuffer utf8;
    private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
    private int position;
    PortableFrameReader(byte[] bytes) { buffer = new UnsafeBuffer(bytes); utf8 = ByteBuffer.wrap(bytes); }
    int remaining() { return buffer.capacity() - position; }
    int take(int bytes) {
        if (bytes < 0 || bytes > remaining()) throw new IllegalArgumentException("truncated frame data");
        int offset = position;
        position += bytes;
        return offset;
    }
    int readByte() { return buffer.getByte(take(1)) & 0xff; }
    int readInt() { return buffer.getInt(take(4), ByteOrder.BIG_ENDIAN); }
    long readLong() { return buffer.getLong(take(8), ByteOrder.BIG_ENDIAN); }
    String readString() {
        int length = readInt();
        int offset = take(length);
        utf8.limit(position).position(offset);
        try { return decoder.decode(utf8).toString(); }
        catch (CharacterCodingException corrupt) { throw new IllegalArgumentException("invalid UTF-8", corrupt); }
    }
}
