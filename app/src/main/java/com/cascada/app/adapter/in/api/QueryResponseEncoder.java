package com.cascada.app.adapter.in.api;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.*;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.*;

/** Encodes the final cache result, so exports preserve cache reconstruction and bypass semantics. */
public final class QueryResponseEncoder {
    /** The one JSON mapper of the inbound API (request parsing and response encoding). */
    static final ObjectMapper JSON = new ObjectMapper();
    private QueryResponseEncoder() {}

    public static void json(ResultFrame frame, OutputStream output) throws IOException {
        FrameColumnReader[] readers = ResultCells.readers(frame);
        SerializedString[] names = frame.columnNames().stream().map(SerializedString::new)
                .toArray(SerializedString[]::new);
        try (JsonGenerator generator = JSON.getFactory().createGenerator(output)) {
            generator.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
            ResultCellVisitor<IOException> cells = new ResultCellVisitor<>() {
                public void nullValue() throws IOException { generator.writeNull(); }
                public void longValue(long value) throws IOException { generator.writeNumber(value); }
                public void doubleValue(double value) throws IOException { generator.writeNumber(value); }
                public void stringValue(String value) throws IOException { generator.writeString(value); }
                public void decimalValue(BigDecimal value) throws IOException { generator.writeNumber(value); }
            };
            generator.writeStartArray();
            for (int row = 0; row < frame.rowCount(); row++) {
                if ((row & 1023) == 0) QueryCancellation.requireCurrentActive();
                generator.writeStartObject();
                for (int column = 0; column < readers.length; column++) {
                    generator.writeFieldName(names[column]);
                    ResultCells.visit(readers[column], row, cells);
                }
                generator.writeEndObject();
            }
            generator.writeEndArray();
        }
    }

    /** Caller deletes the temporary file after transport completes, including on cancellation. */
    public static Path parquet(ResultFrame frame) throws IOException {
        if (frame.columnNames().isEmpty()) throw new IllegalArgumentException("Parquet requires a column schema");
        Types.MessageTypeBuilder schema = Types.buildMessage();
        FrameColumnReader[] readers = ResultCells.readers(frame);
        int[] scales = new int[readers.length];
        for (int c = 0; c < readers.length; c++) {
            String name = frame.columnNames().get(c);
            FrameColumnReader reader = readers[c];
            switch (reader.type()) {
                case LONG -> schema.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
                case DOUBLE -> schema.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
                case STRING -> schema.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).named(name);
                case DECIMAL -> {
                    int scale = 0, integerDigits = 1;
                    for (int r = 0; r < frame.rowCount(); r++) {
                        if ((r & 1023) == 0) QueryCancellation.requireCurrentActive();
                        if (!reader.isNullAt(r)) {
                            BigDecimal value = reader.decimalValue(r);
                            scale = Math.max(scale, value.scale());
                            integerDigits = Math.max(integerDigits, value.precision() - value.scale());
                        }
                    }
                    scales[c] = scale;
                    schema.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                            .as(LogicalTypeAnnotation.decimalType(scale, integerDigits + scale)).named(name);
                }
            }
        }
        MessageType type = schema.named("cascada_result");
        Path directory = Files.createTempDirectory("cascada-parquet-");
        Path file = directory.resolve("result.parquet");
        try {
            var support = new ParquetCellWriter(type, readers, scales);
            try (var writer = new ParquetRowWriterBuilder(new NioParquetFile(file), support)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
                for (int r = 0; r < frame.rowCount(); r++) {
                    if ((r & 1023) == 0) QueryCancellation.requireCurrentActive();
                    writer.write(r);
                }
            }
            return file;
        } catch (IOException | RuntimeException exception) {
            try { deleteParquet(file); }
            catch (IOException cleanup) { exception.addSuppressed(cleanup); }
            throw exception;
        }
    }

    public static void deleteParquet(Path file) throws IOException {
        Files.deleteIfExists(file);
        Files.deleteIfExists(file.getParent());
    }
}
