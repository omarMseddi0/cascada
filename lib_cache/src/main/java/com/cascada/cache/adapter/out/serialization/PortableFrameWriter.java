package com.cascada.cache.adapter.out.serialization;

import java.nio.ByteOrder;
import org.agrona.ExpandableArrayBuffer;

/** Call-owned encoding storage keeps concurrent serializations independent and avoids retained thread-local buffers. */
final class PortableFrameWriter {
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1_024);
    int position;

    byte[] compress(int compressionLevel) {
        return ZstdFrameEnvelope.compress(buffer.byteArray(), position, compressionLevel);
    }

    void writeByte(int value) {
        buffer.putByte(position, (byte) value);
        position = Math.addExact(position, 1);
    }
    void writeInt(int value) {
        buffer.putInt(position, value, ByteOrder.BIG_ENDIAN);
        position = Math.addExact(position, Integer.BYTES);
    }
    void writeLong(long value) {
        buffer.putLong(position, value, ByteOrder.BIG_ENDIAN);
        position = Math.addExact(position, Long.BYTES);
    }
    void writeString(String value) {
        position = Math.addExact(position, buffer.putStringUtf8(position, value, ByteOrder.BIG_ENDIAN));
    }
}
