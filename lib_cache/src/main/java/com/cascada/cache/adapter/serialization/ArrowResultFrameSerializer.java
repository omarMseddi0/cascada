package com.cascada.cache.adapter.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheValueSerializerPort;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

            int rowCount = frame.rowCount();
            root.setRowCount(rowCount);
            for (String column : frame.columnNames()) {
                writeColumn(root.getVector(column), frame.columnType(column), frame.rows(), column);
            }

            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (Exception failure) {
            throw new CacheSerializationException("Arrow frame encoding failed", failure);
        }
    }

    private void writeColumn(FieldVector vector, ColumnType type, List<Map<String, Object>> rows, String column) {
        vector.allocateNew();
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            Object value = rows.get(rowIndex).get(column);
            if (value == null) {
                vector.setNull(rowIndex);
                continue;
            }
            switch (type) {
                case LONG -> ((BigIntVector) vector).setSafe(rowIndex, ((Number) value).longValue());
                case DOUBLE -> ((Float8Vector) vector).setSafe(rowIndex, ((Number) value).doubleValue());
                case STRING -> ((VarCharVector) vector).setSafe(rowIndex, new Text(value.toString()));
            }
        }
        vector.setValueCount(rows.size());
    }

    private ResultFrame decodeFromArrowIpc(byte[] ipc) {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> columnNames = new ArrayList<>();
            Map<String, ColumnType> columnTypes = new LinkedHashMap<>();
            for (Field field : root.getSchema().getFields()) {
                columnNames.add(field.getName());
                columnTypes.put(field.getName(), fromArrowType(field));
            }

            List<Map<String, Object>> rows = new ArrayList<>();
            while (reader.loadNextBatch()) {
                int rowCount = root.getRowCount();
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (String column : columnNames) {
                        row.put(column, readValue(root.getVector(column), columnTypes.get(column), rowIndex));
                    }
                    rows.add(row);
                }
            }
            return new ResultFrame(columnNames, columnTypes, rows);
        } catch (Exception corrupt) {
            throw new CacheSerializationException("Arrow frame decoding failed", corrupt);
        }
    }

    private Object readValue(FieldVector vector, ColumnType type, int rowIndex) {
        if (vector.isNull(rowIndex)) {
            return null;
        }
        return switch (type) {
            case LONG -> ((BigIntVector) vector).get(rowIndex);
            case DOUBLE -> ((Float8Vector) vector).get(rowIndex);
            case STRING -> new String(((VarCharVector) vector).get(rowIndex), java.nio.charset.StandardCharsets.UTF_8);
        };
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
