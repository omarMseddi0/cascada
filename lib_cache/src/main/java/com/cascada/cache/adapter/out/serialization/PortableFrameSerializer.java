package com.cascada.cache.adapter.out.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.github.luben.zstd.Zstd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A dependency-light cache serializer that preserves the exact two-stage contract of
 * {@code cache_serialization.py} — encode the frame to bytes, then zstd-compress at level 9 — without
 * Apache Arrow's JDK-module requirements. It can be injected into environments that cannot open
 * {@code java.base/java.nio}. The app explicitly uses the Arrow IPC serializer
 * ({@link ArrowResultFrameSerializer}) is the language-neutral production alternative behind the same
 * {@link CacheValueSerializerPort}.
 *
 * <p>Blob layout: {@code [4-byte big-endian uncompressed length][zstd level-9 frame]}. A corrupt blob
 * raises {@link CacheSerializationException}, mirroring the Python {@code RuntimeError} semantics.
 */
public final class PortableFrameSerializer implements CacheValueSerializerPort {

    private static final int COMPRESSION_LEVEL = 9;
    private static final int UTF8_FORMAT = -1;

    @Override
    public byte[] serialize(ResultFrame frame) {
        byte[] encoded = encode(frame);
        byte[] compressed = Zstd.compress(encoded, COMPRESSION_LEVEL);
        ByteBuffer blob = ByteBuffer.allocate(Integer.BYTES + compressed.length);
        blob.putInt(encoded.length);
        blob.put(compressed);
        return blob.array();
    }

    @Override
    public ResultFrame deserialize(byte[] blob) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(blob);
            int uncompressedLength = buffer.getInt();
            byte[] compressed = new byte[buffer.remaining()];
            buffer.get(compressed);
            if (uncompressedLength < 0 || Zstd.decompressedSize(compressed) != uncompressedLength) {
                throw new IllegalArgumentException("invalid uncompressed frame length");
            }
            byte[] encoded = Zstd.decompress(compressed, uncompressedLength);
            return decode(encoded);
        } catch (RuntimeException corrupt) {
            throw new CacheSerializationException("cache blob could not be decoded; data may be corrupt", corrupt);
        }
    }

    private byte[] encode(ResultFrame frame) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            List<String> columns = frame.columnNames();
            out.writeInt(UTF8_FORMAT);
            out.writeInt(columns.size());
            for (String column : columns) {
                writeString(out, column);
                out.writeByte(frame.columnType(column).ordinal());
            }
            out.writeInt(frame.rowCount());
            for (int row = 0; row < frame.rowCount(); row++) {
                for (int column = 0; column < columns.size(); column++) {
                    writeValue(out, frame, row, column);
                }
            }
        } catch (IOException impossible) {
            throw new CacheSerializationException("frame encoding failed", impossible);
        }
        return bytes.toByteArray();
    }

    private void writeValue(DataOutputStream out, ResultFrame frame, int row, int column) throws IOException {
        boolean valuePresent = !frame.isNullAt(row, column);
        out.writeBoolean(valuePresent);
        if (!valuePresent) {
            return;
        }
        switch (frame.columnType(frame.columnNames().get(column))) {
            case LONG -> out.writeLong(frame.longAt(row, column));
            case DOUBLE -> out.writeDouble(frame.doubleAt(row, column));
            case STRING -> writeString(out, frame.stringAt(row, column));
            case DECIMAL -> writeString(out, frame.valueAt(row, column).toString());
        }
    }

    private ResultFrame decode(byte[] encoded) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            int columnCount = in.readInt();
            boolean utf8 = columnCount == UTF8_FORMAT;
            if (utf8) columnCount = in.readInt();
            if (columnCount < 0 || columnCount > in.available() / 3) {
                throw new IOException("invalid column count");
            }
            List<String> columnNames = new ArrayList<>(columnCount);
            List<ColumnType> columnTypes = new ArrayList<>(columnCount);
            ResultFrame.Builder builder = ResultFrame.builder();
            for (int index = 0; index < columnCount; index++) {
                String name = readString(in, utf8);
                int typeId = in.readUnsignedByte();
                if (typeId >= ColumnType.values().length) throw new IOException("invalid column type");
                ColumnType type = ColumnType.values()[typeId];
                columnNames.add(name);
                columnTypes.add(type);
                builder.column(name, type);
            }
            int rowCount = in.readInt();
            if (rowCount < 0 || (long) rowCount * columnCount > in.available()
                    || columnCount == 0 && rowCount != 0) {
                throw new IOException("invalid row count");
            }
            for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                for (int column = 0; column < columnNames.size(); column++) {
                    appendValue(in, builder, columnTypes.get(column), utf8);
                }
            }
            if (in.available() != 0) throw new IOException("trailing frame data");
            return builder.build();
        } catch (IOException corrupt) {
            throw new CacheSerializationException("frame decoding failed", corrupt);
        }
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input, boolean utf8) throws IOException {
        if (!utf8) return input.readUTF(); // Existing portable blobs remain readable.
        int length = input.readInt();
        if (length < 0 || length > input.available()) throw new IOException("invalid string length");
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(input.readNBytes(length))).toString();
    }

    private void appendValue(DataInputStream in, ResultFrame.Builder builder, ColumnType type,
                             boolean utf8) throws IOException {
        if (!in.readBoolean()) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case LONG -> builder.appendLong(in.readLong());
            case DOUBLE -> builder.appendDouble(in.readDouble());
            case STRING -> builder.appendString(readString(in, utf8));
            case DECIMAL -> builder.appendDecimal(new java.math.BigDecimal(readString(in, utf8)));
        }
    }
}
