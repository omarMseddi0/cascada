package com.cascada.app.adapter.in.api;

import com.cascada.cache.domain.frame.FrameColumnReader;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

/** Writes typed frame rows directly to Parquet's column writer. */
final class ParquetCellWriter extends WriteSupport<Integer> {
    private final MessageType schema;
    private final FrameColumnReader[] columns;
    private final int[] scales;
    private final String[] names;
    private RecordConsumer consumer;

    ParquetCellWriter(MessageType schema, FrameColumnReader[] columns, int[] scales) {
        this.schema = schema;
        this.columns = columns;
        this.scales = scales;
        names = schema.getFields().stream().map(field -> field.getName()).toArray(String[]::new);
    }

    @Override public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, Map.of());
    }

    @Override public void prepareForWrite(RecordConsumer recordConsumer) {
        consumer = recordConsumer;
    }

    @Override public void write(Integer rowIndex) {
        int row = rowIndex;
        consumer.startMessage();
        for (int index = 0; index < columns.length; index++) {
            FrameColumnReader column = columns[index];
            if (column.isNullAt(row)) continue;
            String field = names[index];
            consumer.startField(field, index);
            switch (column.type()) {
                case LONG -> consumer.addLong(column.longValue(row));
                case DOUBLE -> consumer.addDouble(column.doubleValue(row));
                case STRING -> consumer.addBinary(Binary.fromString(column.stringValue(row)));
                case DECIMAL -> consumer.addBinary(Binary.fromConstantByteArray(
                        column.decimalValue(row).setScale(scales[index]).unscaledValue().toByteArray()));
            }
            consumer.endField(field, index);
        }
        consumer.endMessage();
    }
}
