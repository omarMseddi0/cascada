package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SparkResultFrameMapperTest {

    @Test
    void mapsSupportedScalarTypesAndNullsWithoutStartingSpark() {
        StructType schema = schema(
                field("count", DataTypes.LongType),
                field("ratio", DataTypes.DoubleType),
                field("amount", DataTypes.createDecimalType(12, 2)),
                field("label", DataTypes.StringType),
                field("active", DataTypes.BooleanType),
                field("day", DataTypes.DateType));
        Date day = Date.valueOf("2026-10-03");
        ResultFrame frame = new SparkResultFrameMapper(10).map(schema, List.of(RowFactory.create(
                12L, 0.25d, new BigDecimal("4.20"), "north", true, day),
                RowFactory.create(0L, null, null, null, false, null)).iterator());

        assertThat(frame.columnTypes()).containsEntry("count", ColumnType.LONG)
                .containsEntry("ratio", ColumnType.DOUBLE)
                .containsEntry("amount", ColumnType.DECIMAL)
                .containsEntry("label", ColumnType.STRING)
                .containsEntry("active", ColumnType.STRING)
                .containsEntry("day", ColumnType.STRING);
        assertThat(frame.longAt(0, frame.columnIndex("count"))).isEqualTo(12L);
        assertThat(frame.doubleAt(0, frame.columnIndex("ratio"))).isEqualTo(0.25d);
        assertThat(frame.valueAt(0, frame.columnIndex("amount"))).isEqualTo(new BigDecimal("4.20"));
        assertThat(frame.stringAt(0, frame.columnIndex("active"))).isEqualTo("true");
        assertThat(frame.stringAt(0, frame.columnIndex("day"))).isEqualTo("2026-10-03");
        assertThat(frame.isNullAt(1, frame.columnIndex("ratio"))).isTrue();
        assertThat(frame.isNullAt(1, frame.columnIndex("label"))).isTrue();
    }

    @Test
    void mapsEverySupportedSparkTypeWithItsExactPrimitiveGetterAndExternalStringForm() {
        long exactLong = 9_007_199_254_740_993L;
        BigDecimal exactDecimal = new BigDecimal("123456789012345678.12345678");
        Date day = Date.valueOf("2026-10-03");
        Timestamp timestamp = Timestamp.valueOf("2026-10-03 10:11:12.123456789");
        LocalDateTime timestampNtz = LocalDateTime.of(2026, 10, 3, 10, 11, 12, 123_456_789);
        StructType schema = schema(
                field("byte", DataTypes.ByteType), field("short", DataTypes.ShortType),
                field("integer", DataTypes.IntegerType), field("long", DataTypes.LongType),
                field("float", DataTypes.FloatType), field("double", DataTypes.DoubleType),
                field("decimal", DataTypes.createDecimalType(26, 8)),
                field("string", DataTypes.StringType), field("boolean", DataTypes.BooleanType),
                field("date", DataTypes.DateType), field("timestamp", DataTypes.TimestampType),
                field("timestamp_ntz", DataTypes.TimestampNTZType), field("null", DataTypes.NullType));

        ResultFrame frame = new SparkResultFrameMapper(10).map(schema, List.of(RowFactory.create(
                Byte.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE, exactLong, 0.1f, -7.25d,
                exactDecimal, "north", false, day, timestamp, timestampNtz, null),
                RowFactory.create(null, null, null, null, null, null, null, null, null, null, null, null, null))
                .iterator());

        assertThat(frame.columnNames()).containsExactly("byte", "short", "integer", "long", "float",
                "double", "decimal", "string", "boolean", "date", "timestamp", "timestamp_ntz", "null");
        assertThat(frame.longAt(0, frame.columnIndex("byte"))).isEqualTo(Byte.MIN_VALUE);
        assertThat(frame.longAt(0, frame.columnIndex("short"))).isEqualTo(Short.MAX_VALUE);
        assertThat(frame.longAt(0, frame.columnIndex("integer"))).isEqualTo(Integer.MIN_VALUE);
        assertThat(frame.longAt(0, frame.columnIndex("long"))).isEqualTo(exactLong);
        assertThat(frame.doubleAt(0, frame.columnIndex("float"))).isEqualTo((double) 0.1f);
        assertThat(frame.doubleAt(0, frame.columnIndex("double"))).isEqualTo(-7.25d);
        assertThat(frame.valueAt(0, frame.columnIndex("decimal"))).isEqualTo(exactDecimal);
        assertThat(frame.stringAt(0, frame.columnIndex("string"))).isEqualTo("north");
        assertThat(frame.stringAt(0, frame.columnIndex("boolean"))).isEqualTo("false");
        assertThat(frame.stringAt(0, frame.columnIndex("date"))).isEqualTo(day.toString());
        assertThat(frame.stringAt(0, frame.columnIndex("timestamp"))).isEqualTo(timestamp.toString());
        assertThat(frame.stringAt(0, frame.columnIndex("timestamp_ntz"))).isEqualTo(timestampNtz.toString());
        assertThat(frame.isNullAt(0, frame.columnIndex("null"))).isTrue();
        for (int column = 0; column < schema.size(); column++) {
            assertThat(frame.isNullAt(1, column)).isTrue();
        }
    }

    @Test
    void mapsEmptyIteratorToAnEmptyFrameWithTheDeclaredSchema() {
        StructType schema = schema(field("id", DataTypes.LongType), field("label", DataTypes.StringType));

        ResultFrame frame = new SparkResultFrameMapper(10).map(schema, List.<org.apache.spark.sql.Row>of().iterator());

        assertThat(frame.rowCount()).isZero();
        assertThat(frame.columnNames()).containsExactly("id", "label");
        assertThat(frame.columnTypes()).containsEntry("id", ColumnType.LONG).containsEntry("label", ColumnType.STRING);
    }

    @Test
    void acceptsAResultExactlyAtTheConfiguredRowCeiling() {
        StructType schema = schema(field("id", DataTypes.LongType));

        ResultFrame frame = new SparkResultFrameMapper(1).map(schema, List.of(RowFactory.create(42L)).iterator());

        assertThat(frame.rowCount()).isEqualTo(1);
        assertThat(frame.longAt(0, frame.columnIndex("id"))).isEqualTo(42L);
    }

    @Test
    void rejectsValuesTheFrameContractCannotPreserve() {
        StructType schema = schema(field("payload", DataTypes.BinaryType));

        assertThatThrownBy(() -> new SparkResultFrameMapper(10)
                .map(schema, List.of(RowFactory.create(new byte[]{1, 2, 3})).iterator()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("binary");
    }

    @Test
    void rejectsRowsBeyondTheConfiguredCeiling() {
        StructType schema = schema(field("id", DataTypes.IntegerType));

        assertThatThrownBy(() -> new SparkResultFrameMapper(1).map(schema, List.of(
                RowFactory.create(1), RowFactory.create(2)).iterator()))
                .isInstanceOf(IllegalStateException.class)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("1-row driver ceiling");
    }

    private StructField field(String name, org.apache.spark.sql.types.DataType type) {
        return DataTypes.createStructField(name, type, true);
    }

    private StructType schema(StructField... fields) {
        return DataTypes.createStructType(fields);
    }
}
