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
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

/**
 * The language-neutral production cache serializer: encodes a {@link ResultFrame} as **Apache Arrow IPC
 * (streaming format)** then zstd-compresses at level 9, behind the same {@link CacheValueSerializerPort}
 * as {@link PortableFrameSerializer}. This is the adapter the port's contract has always named (plan
 * §8.16, HARNESS §B.5.5): Arrow is columnar and cross-language, so a bucket written here can be read by
 * Python (pyarrow), Spark, or DuckDB without a bespoke codec — which is what makes the in-process DuckDB
 * roll-up path (Arrow → DuckDB is zero-copy) and Python interop possible.
 *
 * <p>It is a true Liskov substitute for {@link PortableFrameSerializer}: same {@code ColumnType} domain
 * (LONG→{@code BIGINT}, DOUBLE→{@code FLOAT8}, STRING→{@code VARCHAR}), same null handling, same blob
 * envelope {@code [4-byte big-endian uncompressed length][zstd level-9 frame]}, so the in-memory backend,
 * the Valkey backend, and the size accounting all behave identically regardless of which serializer is
 * injected. A corrupt blob raises {@link CacheSerializationException}, mirroring the Python
 * {@code RuntimeError} semantics.
 *
 * <p>Off-heap Arrow buffers are allocated from a per-call {@link RootAllocator} and always freed in a
 * {@code try-with-resources}, so there is no native-memory leak across the millions of bucket
 * (de)serializations a warm cache performs.
 */
public final class ArrowResultFrameSerializer implements CacheValueSerializerPort {

    private static final int COMPRESSION_LEVEL = 9;

    @Override
    public byte[] serialize(ResultFrame frame) {
        byte[] ipc = encodeToArrowIpc(frame);
        byte[] compressed = Zstd.compress(ipc, COMPRESSION_LEVEL);
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
            byte[] compressed = new byte[buffer.remaining()];
            buffer.get(compressed);
            byte[] ipc = Zstd.decompress(compressed, uncompressedLength);
            return decodeFromArrowIpc(ipc);
        } catch (RuntimeException corrupt) {
            throw new CacheSerializationException("Arrow cache blob could not be decoded; data may be corrupt",
                    corrupt);
        }
    }

    private byte[] encodeToArrowIpc(ResultFrame frame) {
        Schema schema = toArrowSchema(frame);
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
             ByteArrayOutputStream out = new ByteArrayOutputStream();
             ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {

            for (int column = 0; column < frame.columnNames().size(); column++) {
                String name = frame.columnNames().get(column);
                writeColumn(root.getVector(name), frame, column);
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

    private void writeColumn(FieldVector vector, ResultFrame frame, int column) {
        vector.allocateNew();
        ColumnType type = frame.columnType(frame.columnNames().get(column));
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            if (frame.isNullAt(rowIndex, column)) {
                vector.setNull(rowIndex);
                continue;
            }
            switch (type) {
                case LONG -> ((BigIntVector) vector).setSafe(rowIndex, frame.longAt(rowIndex, column));
                case DOUBLE -> ((Float8Vector) vector).setSafe(rowIndex, frame.doubleAt(rowIndex, column));
                case STRING -> ((VarCharVector) vector).setSafe(rowIndex, new Text(frame.stringAt(rowIndex, column)));
            }
        }
        vector.setValueCount(frame.rowCount());
    }

    private ResultFrame decodeFromArrowIpc(byte[] ipc) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> columnNames = new ArrayList<>();
            List<ColumnType> columnTypes = new ArrayList<>();
            ResultFrame.Builder builder = ResultFrame.builder();
            for (Field field : root.getSchema().getFields()) {
                columnNames.add(field.getName());
                ColumnType type = fromArrowType(field);
                columnTypes.add(type);
                builder.column(field.getName(), type);
            }

            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    for (int column = 0; column < columnNames.size(); column++) {
                        appendValue(root.getVector(columnNames.get(column)), columnTypes.get(column), rowIndex, builder);
                    }
                }
            }
            return builder.build();
        } catch (Exception corrupt) {
            throw new CacheSerializationException("Arrow frame decoding failed", corrupt);
        }
    }

    private void appendValue(FieldVector vector, ColumnType type, int rowIndex, ResultFrame.Builder builder) {
        if (vector.isNull(rowIndex)) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case LONG -> builder.appendLong(((BigIntVector) vector).get(rowIndex));
            case DOUBLE -> builder.appendDouble(((Float8Vector) vector).get(rowIndex));
            case STRING -> builder.appendString(new String(((VarCharVector) vector).get(rowIndex),
                    java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    private Schema toArrowSchema(ResultFrame frame) {
        List<Field> fields = new ArrayList<>(frame.columnNames().size());
        for (String column : frame.columnNames()) {
            fields.add(new Field(column, FieldType.nullable(toArrowType(frame.columnType(column))), null));
        }
        return new Schema(fields);
    }

    private ArrowType toArrowType(ColumnType type) {
        return switch (type) {
            case LONG -> new ArrowType.Int(64, true);
            case DOUBLE -> new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case STRING -> new ArrowType.Utf8();
        };
    }

    private ColumnType fromArrowType(Field field) {
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
