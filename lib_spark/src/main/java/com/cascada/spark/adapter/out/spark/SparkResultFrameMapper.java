package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;

/** Converts Spark's external rows into the cache's framework-free frame contract. */
final class SparkResultFrameMapper {

    private static final int INITIAL_ROW_CAPACITY = 1_024;
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

    private final int maxResultRows;

    SparkResultFrameMapper(int maxResultRows) {
        if (maxResultRows <= 0) {
            throw new IllegalArgumentException("maxResultRows must be > 0, but was: " + maxResultRows);
        }
        this.maxResultRows = maxResultRows;
    }

    ResultFrame map(StructType schema, Iterator<Row> rows) {
        StructField[] fields = schema.fields();
        ColumnType[] columnTypes = new ColumnType[fields.length];
        byte[] getters = new byte[fields.length];
        ResultFrame.Builder builder = ResultFrame.builder()
                .expectedRows(Math.min(maxResultRows, INITIAL_ROW_CAPACITY));
        for (int index = 0; index < fields.length; index++) {
            ColumnPlan column = columnPlan(fields[index].dataType());
            columnTypes[index] = column.type();
            getters[index] = column.getter();
            builder.column(fields[index].name(), columnTypes[index]);
        }

        int rowCount = 0;
        while (rows.hasNext()) {
            if (rowCount >= maxResultRows) {
                throw new IllegalStateException("query result exceeds the " + maxResultRows
                        + "-row driver ceiling; add a LIMIT or aggregate further");
            }
            Row row = rows.next();
            for (int index = 0; index < getters.length; index++) {
                if (row.isNullAt(index)) {
                    builder.appendNull();
                } else {
                    switch (getters[index]) {
                        case READ_BYTE -> builder.appendLong(row.getByte(index));
                        case READ_SHORT -> builder.appendLong(row.getShort(index));
                        case READ_INT -> builder.appendLong(row.getInt(index));
                        case READ_LONG -> builder.appendLong(row.getLong(index));
                        case READ_FLOAT -> builder.appendDouble(row.getFloat(index));
                        case READ_DOUBLE -> builder.appendDouble(row.getDouble(index));
                        case READ_DECIMAL -> builder.appendDecimal(row.getDecimal(index));
                        case READ_STRING -> builder.appendString(row.getString(index));
                        case READ_BOOLEAN -> builder.appendString(Boolean.toString(row.getBoolean(index)));
                        case READ_EXTERNAL_STRING -> builder.appendString(String.valueOf(row.get(index)));
                        default -> throw new AssertionError("unknown Spark result getter " + getters[index]);
                    }
                }
            }
            rowCount++;
        }
        return builder.build();
    }

    private ColumnPlan columnPlan(DataType dataType) {
        if (dataType.equals(DataTypes.ByteType)) {
            return new ColumnPlan(ColumnType.LONG, READ_BYTE);
        }
        if (dataType.equals(DataTypes.ShortType)) {
            return new ColumnPlan(ColumnType.LONG, READ_SHORT);
        }
        if (dataType.equals(DataTypes.IntegerType)) {
            return new ColumnPlan(ColumnType.LONG, READ_INT);
        }
        if (dataType.equals(DataTypes.LongType)) {
            return new ColumnPlan(ColumnType.LONG, READ_LONG);
        }
        if (dataType.equals(DataTypes.FloatType)) {
            return new ColumnPlan(ColumnType.DOUBLE, READ_FLOAT);
        }
        if (dataType.equals(DataTypes.DoubleType)) {
            return new ColumnPlan(ColumnType.DOUBLE, READ_DOUBLE);
        }
        if (dataType instanceof DecimalType) {
            return new ColumnPlan(ColumnType.DECIMAL, READ_DECIMAL);
        }
        if (dataType.equals(DataTypes.StringType)) {
            return new ColumnPlan(ColumnType.STRING, READ_STRING);
        }
        if (dataType.equals(DataTypes.BooleanType)) {
            return new ColumnPlan(ColumnType.STRING, READ_BOOLEAN);
        }
        if (dataType.equals(DataTypes.DateType) || dataType.equals(DataTypes.TimestampType)
                || dataType.equals(DataTypes.TimestampNTZType) || dataType.equals(DataTypes.NullType)) {
            // Preserve Spark's external date/time representation (java.sql or java.time) exactly.
            return new ColumnPlan(ColumnType.STRING, READ_EXTERNAL_STRING);
        }
        throw new UnsupportedOperationException("Spark result type '" + dataType.catalogString()
                + "' cannot be represented by a cache result frame");
    }

    private record ColumnPlan(ColumnType type, byte getter) { }
}
