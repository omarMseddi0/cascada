package com.cascada.cache.adapter.out.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.github.luben.zstd.Zstd;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * The language-neutral production cache serializer: encodes a {@link ResultFrame} as **Apache Arrow IPC
 * (streaming format)** then zstd-compresses at a configurable level, behind the same {@link CacheValueSerializerPort}
 * as {@link PortableFrameSerializer}. This is the adapter the port's contract has always named (plan
 * §8.16, HARNESS §B.5.5): Arrow is columnar and cross-language, so a bucket written here can be read by
 * Python (pyarrow), Spark, or DuckDB without a bespoke codec — which is what makes the in-process DuckDB
 * roll-up path (Arrow → DuckDB is zero-copy) and Python interop possible.
 *
 * <p>It is a true Liskov substitute for {@link PortableFrameSerializer}: same {@code ColumnType} domain
 * (LONG→{@code BIGINT}, DOUBLE→{@code FLOAT8}, STRING→{@code VARCHAR}), same null handling, same blob
 * envelope {@code [4-byte big-endian uncompressed length][zstd frame]}, so the in-memory backend,
 * the Valkey backend, and the size accounting all behave identically regardless of which serializer is
 * injected. A corrupt blob raises {@link CacheSerializationException}, mirroring the Python
 * {@code RuntimeError} semantics.
 *
 * <p>Off-heap Arrow buffers are allocated from a per-call {@link RootAllocator} and always freed in a
 * {@code try-with-resources}, so there is no native-memory leak across the millions of bucket
 * (de)serializations a warm cache performs.
 */
public final class ArrowResultFrameSerializer implements CacheValueSerializerPort {

    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    private final int compressionLevel;

    public ArrowResultFrameSerializer() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ArrowResultFrameSerializer(int compressionLevel) {
        if (compressionLevel < Zstd.minCompressionLevel() || compressionLevel > Zstd.maxCompressionLevel()) {
            throw new IllegalArgumentException("compressionLevel is outside the supported Zstandard range");
        }
        this.compressionLevel = compressionLevel;
    }

    @Override
    public byte[] serialize(ResultFrame frame) {
        if (frame.rowCount() >= 4_096) return encodeCompressedArrow(frame);
        byte[] ipc = encodeToArrowIpc(frame);
        byte[] compressed = Zstd.compress(ipc, compressionLevel);
        ByteBuffer blob = ByteBuffer.allocate(Integer.BYTES + compressed.length);
        blob.putInt(ipc.length);
        blob.put(compressed);
        return blob.array();
    }

    @Override
    public ResultFrame deserialize(byte[] blob) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(blob);
            int uncompressedLength = buffer.getInt();
            int compressedOffset = buffer.position();
            int compressedLength = buffer.remaining();
            if (uncompressedLength < 0
                    || Zstd.decompressedSize(blob, compressedOffset, compressedLength) != uncompressedLength) {
                throw new IllegalArgumentException("invalid uncompressed frame length");
            }
            byte[] ipc = new byte[uncompressedLength];
            long decompressedBytes = Zstd.decompressByteArray(ipc, 0, uncompressedLength,
                    blob, compressedOffset, compressedLength);
            if (Zstd.isError(decompressedBytes) || decompressedBytes != uncompressedLength) {
                throw new IllegalArgumentException("invalid compressed frame data: "
                        + Zstd.getErrorName(decompressedBytes));
            }
            return decodeFromArrowIpc(ipc);
        } catch (RuntimeException corrupt) {
            throw new CacheSerializationException("Arrow cache blob could not be decoded; data may be corrupt",
                    corrupt);
        }
    }

    private byte[] encodeCompressedArrow(ResultFrame frame) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(toArrowSchema(frame), allocator)) {
            populate(root, frame);
            CountingChannel count = new CountingChannel();
            writeIpc(root, count);
            int bytes = Math.toIntExact(count.bytes);
            try (ArrowZstdChannel compressed = new ArrowZstdChannel(allocator, bytes, compressionLevel)) {
                writeIpc(root, compressed);
                return compressed.blob();
            }
        } catch (Exception failure) {
            throw new CacheSerializationException("Arrow frame encoding failed", failure);
        }
    }

    private void writeIpc(VectorSchemaRoot root, WritableByteChannel channel) throws IOException {
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, channel)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    private void populate(VectorSchemaRoot root, ResultFrame frame) {
        for (int column = 0; column < frame.columnNames().size(); column++) {
            FieldVector vector = root.getVector(frame.columnNames().get(column));
            vector.setInitialCapacity(frame.rowCount());
            writeColumn(vector, frame, column, frame.columnTypeAt(column));
        }
        root.setRowCount(frame.rowCount());
    }

    private static final class CountingChannel implements WritableByteChannel {
        private long bytes;
        private boolean open = true;
        @Override public int write(ByteBuffer source) {
            int size = source.remaining();
            bytes = Math.addExact(bytes, size);
            source.position(source.limit());
            return size;
        }
        @Override public boolean isOpen() { return open; }
        @Override public void close() { open = false; }
    }

    private byte[] encodeToArrowIpc(ResultFrame frame) {
        Schema schema = toArrowSchema(frame);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            for (int column = 0; column < frame.columnNames().size(); column++) {
                FieldVector vector = root.getVector(frame.columnNames().get(column));
                vector.setInitialCapacity(frame.rowCount());
                writeColumn(vector, frame, column, frame.columnTypeAt(column));
            }
            // Set the row count AFTER populating the vectors (Arrow's contract); setting it first leaves
            // the writer expecting buffers that the vectors have not filled yet.
            root.setRowCount(frame.rowCount());

            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (Exception failure) {
            throw new CacheSerializationException("Arrow frame encoding failed", failure);
        }
    }

    private void writeColumn(FieldVector vector, ResultFrame frame, int column, ColumnType type) {
        vector.allocateNew();
        ResultFrame.ColumnReader values = frame.columnReader(column);
        Utf8EncodingCache strings = type == ColumnType.STRING ? new Utf8EncodingCache(frame.rowCount()) : null;
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            if (values.isNullAt(rowIndex)) {
                vector.setNull(rowIndex);
                continue;
            }
            switch (type) {
                case LONG -> ((BigIntVector) vector).setSafe(rowIndex, values.longValue(rowIndex));
                case DOUBLE -> ((Float8Vector) vector).setSafe(rowIndex, values.doubleValue(rowIndex));
                case STRING -> ((VarCharVector) vector).setSafe(rowIndex, strings.encode(values.stringValue(rowIndex)));
                case DECIMAL -> ((VarCharVector) vector).setSafe(rowIndex,
                        values.decimalValue(rowIndex).toString().getBytes(StandardCharsets.UTF_8));
            }
        }
        vector.setValueCount(frame.rowCount());
    }

    private ResultFrame decodeFromArrowIpc(byte[] ipc) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<Field> fields = root.getSchema().getFields();
            FieldVector[] vectors = new FieldVector[fields.size()];
            ColumnType[] columnTypes = new ColumnType[fields.size()];
            Utf8ValueCache[] strings = new Utf8ValueCache[fields.size()];
            ResultFrame.Builder builder = ResultFrame.builder();
            for (int column = 0; column < fields.size(); column++) {
                Field field = fields.get(column);
                ColumnType type = fromArrowType(field);
                vectors[column] = root.getVector(field.getName());
                columnTypes[column] = type;
                builder.column(field.getName(), type);
            }

            boolean capacityHintSet = false;
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                if (rowCount > 0) {
                    for (int column = 0; column < fields.size(); column++) {
                        if (columnTypes[column] == ColumnType.STRING && strings[column] == null) {
                            strings[column] = new Utf8ValueCache(rowCount >= 4_096 ? 4_096 : Math.min(64, rowCount));
                        }
                    }
                }
                if (!capacityHintSet) {
                    builder.expectedRows(rowCount);
                    capacityHintSet = true;
                }
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    for (int column = 0; column < fields.size(); column++) {
                        appendValue(vectors[column], columnTypes[column], rowIndex, builder, strings[column]);
                    }
                }
            }
            return builder.build();
        } catch (Exception corrupt) {
            throw new CacheSerializationException("Arrow frame decoding failed", corrupt);
        }
    }

    private void appendValue(FieldVector vector, ColumnType type, int rowIndex, ResultFrame.Builder builder,
                             Utf8ValueCache strings) {
        if (vector.isNull(rowIndex)) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case DECIMAL -> builder.appendDecimal(new java.math.BigDecimal(new String(((VarCharVector) vector).get(rowIndex), java.nio.charset.StandardCharsets.UTF_8)));
            case LONG -> builder.appendLong(((BigIntVector) vector).get(rowIndex));
            case DOUBLE -> builder.appendDouble(((Float8Vector) vector).get(rowIndex));
            case STRING -> builder.appendString(strings.read((VarCharVector) vector, rowIndex));
        }
    }

    /** Bounded per-column encoding reuse; predominantly distinct columns leave the cache early. */
    private static final class Utf8EncodingCache {
        private final Map<String, byte[]> values = new HashMap<>();
        private int reads;
        private int hits;
        private int bytes;
        private boolean disabled;
        private final int sampleSize;

        private Utf8EncodingCache(int rows) { sampleSize = rows >= 4_096 ? 4_096 : Math.max(1, Math.min(64, rows)); }

        private byte[] encode(String value) {
            if (disabled) return value.getBytes(StandardCharsets.UTF_8);
            reads++;
            byte[] encoded = values.get(value);
            if (encoded != null) { hits++; return encoded; }
            encoded = value.getBytes(StandardCharsets.UTF_8);
            if (reads >= sampleSize && (long) hits * 10 < reads) {
                values.clear();
                disabled = true;
            } else if (values.size() < 16_384 && encoded.length <= 1_048_576 - bytes) {
                values.put(value, encoded);
                bytes += encoded.length;
            }
            return encoded;
        }
    }

    private Schema toArrowSchema(ResultFrame frame) {
        List<Field> fields = new ArrayList<>(frame.columnNames().size());
        for (String column : frame.columnNames()) {
            fields.add(new Field(column, new FieldType(true, toArrowType(frame.columnType(column)), null, frame.columnType(column) == ColumnType.DECIMAL ? java.util.Map.of("cascada.type", "decimal") : null), null));
        }
        return new Schema(fields);
    }

    private ArrowType toArrowType(ColumnType type) {
        return switch (type) {
            case LONG -> new ArrowType.Int(64, true);
            case DOUBLE -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case STRING, DECIMAL -> new ArrowType.Utf8();
        };
    }

    private ColumnType fromArrowType(Field field) {
        if ("decimal".equals(field.getMetadata().get("cascada.type"))) return ColumnType.DECIMAL;
        ArrowType arrowType = field.getType();
        if (arrowType instanceof ArrowType.Int) {
            return ColumnType.LONG;
        }
        if (arrowType instanceof ArrowType.FloatingPoint) {
            return ColumnType.DOUBLE;
        }
        if (arrowType instanceof ArrowType.Utf8) {
            return ColumnType.STRING;
        }
        throw new CacheSerializationException("unsupported Arrow type for column '" + field.getName()
                + "': " + arrowType, null);
    }
}
