package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Evaluates select expressions over a merged cache frame with Spark's own expression semantics.
 * The frame becomes an in-memory relation; Spark folds the projection into it on the driver, so
 * no job is submitted to executors.
 */
final class MergedFrameProjection {

    private final SparkSession session;
    private final SparkResultFrameMapper mapper;

    MergedFrameProjection(SparkSession session, SparkResultFrameMapper mapper) {
        this.session = session;
        this.mapper = mapper;
    }

    ResultFrame project(ResultFrame input, Map<String, String> declaredTypes, List<String> projections) {
        int columnCount = input.columnNames().size();
        DataType[] types = new DataType[columnCount];
        StructField[] fields = new StructField[columnCount];
        for (int column = 0; column < columnCount; column++) {
            String name = input.columnNames().get(column);
            String declared = declaredTypes.get(name);
            types[column] = declared != null ? DataType.fromDDL(declared) : frameType(input, column);
            fields[column] = DataTypes.createStructField(name, types[column], true);
        }

        List<Row> rows = new ArrayList<>(input.rowCount());
        for (int row = 0; row < input.rowCount(); row++) {
            Object[] values = new Object[columnCount];
            for (int column = 0; column < columnCount; column++) {
                values[column] = sparkValue(input.valueAt(row, column), types[column]);
            }
            rows.add(RowFactory.create(values));
        }

        Dataset<Row> projected = session.createDataFrame(rows, new StructType(fields))
                .selectExpr(projections.toArray(new String[0]));
        return mapper.map(projected.schema(), projected.collectAsList().iterator());
    }

    private static DataType frameType(ResultFrame input, int column) {
        ColumnType type = input.columnTypeAt(column);
        return switch (type) {
            case LONG -> DataTypes.LongType;
            case DOUBLE -> DataTypes.DoubleType;
            case STRING -> DataTypes.StringType;
            case DECIMAL -> DataTypes.createDecimalType(DecimalType.MAX_PRECISION(), maximumScale(input, column));
        };
    }

    private static int maximumScale(ResultFrame input, int column) {
        int scale = 0;
        for (int row = 0; row < input.rowCount(); row++) {
            if (!input.isNullAt(row, column)) {
                scale = Math.max(scale, ((BigDecimal) input.valueAt(row, column)).scale());
            }
        }
        return Math.min(scale, DecimalType.MAX_SCALE());
    }

    /** Converts a frame cell to the Java value Spark expects for the declared column type. */
    private static Object sparkValue(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        if (type instanceof DecimalType decimal) {
            // The merge keeps Spark's scale exactly; a mismatch is an error, not a rounding.
            return ((BigDecimal) value).setScale(decimal.scale(), RoundingMode.UNNECESSARY);
        }
        if (type.equals(DataTypes.LongType)) {
            return ((Number) value).longValue();
        }
        if (type.equals(DataTypes.IntegerType)) {
            return Math.toIntExact(((Number) value).longValue());
        }
        if (type.equals(DataTypes.ShortType)) {
            return (short) Math.toIntExact(((Number) value).longValue());
        }
        if (type.equals(DataTypes.ByteType)) {
            return (byte) Math.toIntExact(((Number) value).longValue());
        }
        if (type.equals(DataTypes.DoubleType)) {
            return ((Number) value).doubleValue();
        }
        if (type.equals(DataTypes.FloatType)) {
            return ((Number) value).floatValue();
        }
        if (type.equals(DataTypes.StringType)) {
            return value.toString();
        }
        if (type.equals(DataTypes.BooleanType)) {
            return Boolean.parseBoolean(value.toString());
        }
        throw new IllegalArgumentException("merged column type " + type.catalogString() + " cannot be re-evaluated");
    }
}
