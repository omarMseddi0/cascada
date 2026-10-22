package com.cascada.app.adapter.in.api;

import com.google.protobuf.ByteString;
import java.io.OutputStream;
import java.util.function.Consumer;

final class GrpcChunkOutputStream extends OutputStream {
    private byte[] buffer = new byte[64 * 1024];
    private final Consumer<ByteString> send;
    private int position;
    GrpcChunkOutputStream(Consumer<ByteString> send) { this.send = send; }
    @Override public void write(int value) {
        buffer[position++] = (byte) value;
        if (position == buffer.length) flush();
    }
    @Override public void write(byte[] bytes, int offset, int length) {
        int remaining = length;
        while (remaining > 0) {
            int count = Math.min(remaining, buffer.length - position);
            System.arraycopy(bytes, offset + length - remaining, buffer, position, count);
            position += count; remaining -= count;
            if (position == buffer.length) flush();
        }
    }
    @Override public void flush() {
        if (position > 0) {
            byte[] ownedBuffer = buffer;
            int length = position;
            buffer = new byte[64 * 1024]; position = 0;
            // Ownership transfers to protobuf; this array is never reused or mutated after publication.
            send.accept(com.google.protobuf.UnsafeByteOperations.unsafeWrap(ownedBuffer, 0, length));
        }
    }
    @Override public void close() { flush(); }
}
