package com.cascada.cache.adapter.out.serialization;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/** Counts the Arrow IPC bytes written during the sizing pass without retaining the stream. */
final class CountingChannel implements WritableByteChannel {
    long bytes;
    boolean open = true;
    @Override public int write(ByteBuffer source) {
        int size = source.remaining();
        bytes = Math.addExact(bytes, size);
        source.position(source.limit());
        return size;
    }
    @Override public boolean isOpen() { return open; }
    @Override public void close() { open = false; }
}
