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
                .hasMessageContaining("1-row driver ceiling");
    }

    private StructField field(String name, org.apache.spark.sql.types.DataType type) {
        return DataTypes.createStructField(name, type, true);
    }

    private StructType schema(StructField... fields) {
        return DataTypes.createStructType(fields);
    }
}
