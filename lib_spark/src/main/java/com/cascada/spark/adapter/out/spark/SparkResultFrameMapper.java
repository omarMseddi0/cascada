package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

/** Converts collected Spark rows into the cache's framework-free frame contract. */
final class SparkResultFrameMapper {

    private static final byte READ_BYTE = 0;
    private static final byte READ_SHORT = 1;
    private static final byte READ_INT = 2;
    private static final byte READ_LONG = 3;
    private static final byte READ_FLOAT = 4;
    private static final byte READ_DOUBLE = 5;
    private static final byte READ_DECIMAL = 6;
    private static final byte READ_STRING = 7;
    private static final byte READ_BOOLEAN = 8;
    private static final byte READ_EXTERNAL_STRING = 9;

    /**
     * Reads Spark's internal rows directly: primitives without boxing, strings straight from UTF-8, and only
     * date/time columns through Spark's own external conversion so their text form matches {@code collect()}.
     */
    ResultFrame map(StructType schema, InternalRow[] rows) {
        StructField[] fields = schema.fields();
        byte[] getters = new byte[fields.length];
        List<Function1<Object, Object>> externalConverters = new ArrayList<>(fields.length);
        ResultFrameBuilder builder = ResultFrame.builder().expectedRows(rows.length);
        for (int index = 0; index < fields.length; index++) {
            SparkColumnPlan column = columnPlan(fields[index].dataType());
            getters[index] = column.getter();
            externalConverters.add(column.getter() == READ_EXTERNAL_STRING
                    ? CatalystTypeConverters.createToScalaConverter(fields[index].dataType())
                    : null);
            builder.column(fields[index].name(), column.type());
        }

        var cancellation = QueryExecutionScope.current().orElse(null);
        for (InternalRow row : rows) {
            if (cancellation != null) cancellation.requireActive();
            for (int index = 0; index < getters.length; index++) {
                if (row.isNullAt(index)) {
                    builder.appendNull();
                    continue;
                }
                DataType dataType = fields[index].dataType();
                switch (getters[index]) {
                    case READ_BYTE -> builder.appendLong(row.getByte(index));
                    case READ_SHORT -> builder.appendLong(row.getShort(index));
                    case READ_INT -> builder.appendLong(row.getInt(index));
                    case READ_LONG -> builder.appendLong(row.getLong(index));
                    case READ_FLOAT -> builder.appendDouble(row.getFloat(index));
                    case READ_DOUBLE -> builder.appendDouble(row.getDouble(index));
                    case READ_DECIMAL -> {
                        DecimalType decimalType = (DecimalType) dataType;
                        builder.appendDecimal(row.getDecimal(index, decimalType.precision(), decimalType.scale())
                                .toJavaBigDecimal());
                    }
                    case READ_STRING -> builder.appendString(row.getUTF8String(index).toString());
                    case READ_BOOLEAN -> builder.appendString(Boolean.toString(row.getBoolean(index)));
                    case READ_EXTERNAL_STRING -> builder.appendString(String.valueOf(
                            externalConverters.get(index).apply(row.get(index, dataType))));
                    default -> throw new AssertionError("unknown Spark result getter " + getters[index]);
                }
            }
        }
        return builder.build();
    }

    private SparkColumnPlan columnPlan(DataType dataType) {
        if (dataType.equals(DataTypes.ByteType)) {
            return new SparkColumnPlan(ColumnType.LONG, READ_BYTE);
        }
        if (dataType.equals(DataTypes.ShortType)) {
            return new SparkColumnPlan(ColumnType.LONG, READ_SHORT);
        }
        if (dataType.equals(DataTypes.IntegerType)) {
            return new SparkColumnPlan(ColumnType.LONG, READ_INT);
        }
        if (dataType.equals(DataTypes.LongType)) {
            return new SparkColumnPlan(ColumnType.LONG, READ_LONG);
        }
        if (dataType.equals(DataTypes.FloatType)) {
            return new SparkColumnPlan(ColumnType.DOUBLE, READ_FLOAT);
        }
        if (dataType.equals(DataTypes.DoubleType)) {
            return new SparkColumnPlan(ColumnType.DOUBLE, READ_DOUBLE);
        }
        if (dataType instanceof DecimalType) {
            return new SparkColumnPlan(ColumnType.DECIMAL, READ_DECIMAL);
        }
        if (dataType.equals(DataTypes.StringType)) {
            return new SparkColumnPlan(ColumnType.STRING, READ_STRING);
        }
        if (dataType.equals(DataTypes.BooleanType)) {
            return new SparkColumnPlan(ColumnType.STRING, READ_BOOLEAN);
        }
        if (dataType.equals(DataTypes.DateType) || dataType.equals(DataTypes.TimestampType)
                || dataType.equals(DataTypes.TimestampNTZType) || dataType.equals(DataTypes.NullType)) {
            // Preserve Spark's external date/time representation (java.sql or java.time) exactly.
            return new SparkColumnPlan(ColumnType.STRING, READ_EXTERNAL_STRING);
        }
        throw new UnsupportedOperationException("Spark result type '" + dataType.catalogString()
                + "' cannot be represented by a cache result frame");
    }

}
