package com.cascada.app.adapter.in.api;

import com.cascada.cache.domain.frame.ResultFrame;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

/** Encodes the final cache result, so exports preserve cache reconstruction and bypass semantics. */
public final class QueryResponseEncoder {
    /** The one JSON mapper of the inbound API (request parsing and response encoding). */
    static final ObjectMapper JSON = new ObjectMapper();
    private QueryResponseEncoder() {}

    public static void json(ResultFrame frame, OutputStream output) throws IOException {
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
                generator.writeStartObject();
                for (int column = 0; column < frame.columnNames().size(); column++) {
                    generator.writeFieldName(frame.columnNames().get(column));
                    ResultCells.visit(frame, row, column, cells);
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
        int count = frame.columnNames().size();
        int[] scales = new int[count];
        for (int c = 0; c < count; c++) {
            String name = frame.columnNames().get(c);
            switch (frame.columnTypeAt(c)) {
                case LONG -> schema.optional(PrimitiveType.PrimitiveTypeName.INT64).named(name);
                case DOUBLE -> schema.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(name);
                case STRING -> schema.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.stringType()).named(name);
                default -> {
                    int scale = 0, integerDigits = 1;
                    for (int r = 0; r < frame.rowCount(); r++) {
                        if (!frame.isNullAt(r, c)) {
                            BigDecimal value = (BigDecimal) frame.valueAt(r, c);
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
        SimpleGroupFactory groups = new SimpleGroupFactory(type);
        Path directory = Files.createTempDirectory("cascada-parquet-");
        Path file = directory.resolve("result.parquet");
        try {
            Configuration config = new Configuration();
            try (var writer = ExampleParquetWriter.builder(new NioParquetFile(file))
                    .withConf(config).withType(type)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED).build()) {
                ParquetCellWriter cells = new ParquetCellWriter(scales);
                for (int r = 0; r < frame.rowCount(); r++) {
                    cells.startRecord(groups.newGroup());
                    for (int c = 0; c < count; c++) {
                        cells.field(c);
                        ResultCells.visit(frame, r, c, cells);
                    }
                    writer.write(cells.record());
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
