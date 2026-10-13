package com.cascada.cache.adapter.out.serialization;

import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * A dependency-light cache serializer that preserves the exact two-stage contract of
 * {@code cache_serialization.py} — encode the frame to bytes, then zstd-compress at a configurable level — without
 * Apache Arrow's JDK-module requirements. It can be injected into environments that cannot open
 * {@code java.base/java.nio}. The app explicitly uses the Arrow IPC serializer
 * ({@link ArrowResultFrameSerializer}) is the language-neutral production alternative behind the same
 * {@link CacheValueSerializerPort}.
 *
 * <p>Blob layout: {@code [4-byte big-endian uncompressed length][zstd frame]}. A corrupt blob
 * raises {@link CacheSerializationException}, mirroring the Python {@code RuntimeError} semantics.
 */
public final class PortableFrameSerializer implements CacheValueSerializerPort {

    public static final int DEFAULT_COMPRESSION_LEVEL = 3;
    private static final int UTF8_FORMAT = -1;

    private final int compressionLevel;

    public PortableFrameSerializer() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public PortableFrameSerializer(int compressionLevel) {
        if (compressionLevel < Zstd.minCompressionLevel() || compressionLevel > Zstd.maxCompressionLevel()) {
            throw new IllegalArgumentException("compressionLevel is outside the supported Zstandard range");
        }
        this.compressionLevel = compressionLevel;
    }

    @Override
    public byte[] serialize(ResultFrame frame) {
        PortableFrameWriter out = encode(frame);
        return out.compress(compressionLevel);
    }

    @Override
    public ResultFrame deserialize(byte[] blob) {
        try {
            byte[] encoded = ZstdFrameEnvelope.decompress(blob);
            return decode(encoded);
        } catch (RuntimeException corrupt) {
            throw new CacheSerializationException("cache blob could not be decoded; data may be corrupt", corrupt);
        }
    }

    private PortableFrameWriter encode(ResultFrame frame) {
        PortableFrameWriter out = new PortableFrameWriter();
        List<String> columns = frame.columnNames();
        FrameColumnReader[] readers = new FrameColumnReader[columns.size()];
        out.writeInt(UTF8_FORMAT);
        out.writeInt(columns.size());
        for (int column = 0; column < columns.size(); column++) {
            out.writeString(columns.get(column));
            readers[column] = frame.frameColumnReader(column);
            out.writeByte(readers[column].type().ordinal());
        }
        out.writeInt(frame.rowCount());
        for (int row = 0; row < frame.rowCount(); row++) {
            for (FrameColumnReader values : readers) {
                boolean present = !values.isNullAt(row);
                out.writeByte(present ? 1 : 0);
                if (!present) continue;
                switch (values.type()) {
                    case LONG -> out.writeLong(values.longValue(row));
                    // DataOutputStream canonicalises NaN; preserve the existing wire bytes.
                    case DOUBLE -> out.writeLong(Double.doubleToLongBits(values.doubleValue(row)));
                    case STRING -> out.writeString(values.stringValue(row));
                    case DECIMAL -> out.writeString(values.decimalValue(row).toString());
                }
            }
        }
        return out;
    }

    private ResultFrame decode(byte[] encoded) {
        PortableFrameReader in = new PortableFrameReader(encoded);
        if (in.readInt() != UTF8_FORMAT) return decodeLegacy(encoded);
        int columns = in.readInt();
        if (columns < 0 || columns > in.remaining() / 5) throw new IllegalArgumentException("invalid column count");
        ColumnType[] types = new ColumnType[columns];
        ColumnType[] knownTypes = ColumnType.values();
        ResultFrameBuilder builder = new ResultFrameBuilder();
        for (int column = 0; column < columns; column++) {
            String name = in.readString();
            int type = in.readByte();
            if (type >= knownTypes.length) throw new IllegalArgumentException("invalid column type");
            types[column] = knownTypes[type];
            builder.column(name, types[column]);
        }
        int rows = in.readInt();
        if (rows < 0 || (long) rows * columns > in.remaining() || columns == 0 && rows != 0) {
            throw new IllegalArgumentException("invalid row count");
        }
        builder.expectedRows(rows);
        for (int row = 0; row < rows; row++) {
            for (ColumnType type : types) {
                if (in.readByte() == 0) builder.appendNull();
                else switch (type) {
                    case LONG -> builder.appendLong(in.readLong());
                    case DOUBLE -> builder.appendDouble(Double.longBitsToDouble(in.readLong()));
                    case STRING -> builder.appendString(in.readString());
                    case DECIMAL -> builder.appendDecimal(new java.math.BigDecimal(in.readString()));
                }
            }
        }
        if (in.remaining() != 0) throw new IllegalArgumentException("trailing frame data");
        return builder.build();
    }

    /** Only pre-UTF8 portable blobs need Java's modified-UTF codec. */
    private ResultFrame decodeLegacy(byte[] encoded) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(encoded))) {
            int columnCount = in.readInt();
            if (columnCount < 0 || columnCount > in.available() / 3) throw new IOException("invalid column count");
            ColumnType[] types = new ColumnType[columnCount];
            ColumnType[] knownTypes = ColumnType.values();
            ResultFrameBuilder builder = new ResultFrameBuilder();
            for (int column = 0; column < columnCount; column++) {
                String name = in.readUTF();
                int type = in.readUnsignedByte();
                if (type >= knownTypes.length) throw new IOException("invalid column type");
                types[column] = knownTypes[type];
                builder.column(name, types[column]);
            }
            int rows = in.readInt();
            if (rows < 0 || (long) rows * columnCount > in.available() || columnCount == 0 && rows != 0) {
                throw new IOException("invalid row count");
            }
            builder.expectedRows(rows);
            for (int row = 0; row < rows; row++) {
                for (ColumnType type : types) {
                    if (!in.readBoolean()) builder.appendNull();
                    else switch (type) {
                        case LONG -> builder.appendLong(in.readLong());
                        case DOUBLE -> builder.appendDouble(in.readDouble());
                        case STRING -> builder.appendString(in.readUTF());
                        case DECIMAL -> builder.appendDecimal(new java.math.BigDecimal(in.readUTF()));
                    }
                }
            }
            if (in.available() != 0) throw new IOException("trailing frame data");
            return builder.build();
        } catch (IOException corrupt) {
            throw new CacheSerializationException("frame decoding failed", corrupt);
        }
    }
}
