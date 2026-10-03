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
        ResultFrame.Builder builder = ResultFrame.builder();
        for (int index = 0; index < fields.length; index++) {
            columnTypes[index] = mapSparkType(fields[index].dataType());
            builder.column(fields[index].name(), columnTypes[index]);
        }

        int rowCount = 0;
        while (rows.hasNext()) {
            if (rowCount >= maxResultRows) {
                throw new IllegalStateException("query result exceeds the " + maxResultRows
                        + "-row driver ceiling; add a LIMIT or aggregate further");
            }
            Row row = rows.next();
            for (int index = 0; index < fields.length; index++) {
                appendCell(builder, row, index, columnTypes[index]);
            }
            rowCount++;
        }
        return builder.build();
    }

    private void appendCell(ResultFrame.Builder builder, Row row, int index, ColumnType type) {
        if (row.isNullAt(index)) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case DECIMAL -> builder.appendDecimal(row.getDecimal(index));
            case LONG -> builder.appendLong(((Number) row.get(index)).longValue());
            case DOUBLE -> builder.appendDouble(((Number) row.get(index)).doubleValue());
            case STRING -> builder.appendString(String.valueOf(row.get(index)));
        }
    }

    private ColumnType mapSparkType(DataType dataType) {
        if (dataType.equals(DataTypes.ByteType) || dataType.equals(DataTypes.ShortType)
                || dataType.equals(DataTypes.IntegerType) || dataType.equals(DataTypes.LongType)) {
            return ColumnType.LONG;
        }
        if (dataType.equals(DataTypes.FloatType) || dataType.equals(DataTypes.DoubleType)) {
            return ColumnType.DOUBLE;
        }
        if (dataType instanceof DecimalType) {
            return ColumnType.DECIMAL;
        }
        if (dataType.equals(DataTypes.StringType) || dataType.equals(DataTypes.BooleanType)
                || dataType.equals(DataTypes.DateType) || dataType.equals(DataTypes.TimestampType)
                || dataType.equals(DataTypes.TimestampNTZType) || dataType.equals(DataTypes.NullType)) {
            return ColumnType.STRING;
        }
        throw new UnsupportedOperationException("Spark result type '" + dataType.catalogString()
                + "' cannot be represented by a cache result frame");
    }
}
