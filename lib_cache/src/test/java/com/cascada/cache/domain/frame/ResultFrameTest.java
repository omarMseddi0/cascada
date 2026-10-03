package com.cascada.cache.domain.frame;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResultFrameTest {

    @Test
    void resolvedColumnReaderKeepsItsSnapshotAcrossBuilderReuse() {
        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(64).column("n", ColumnType.LONG);
        for (int row = 0; row < 64; row++) builder.appendLong(row);
        ResultFrame.ColumnReader reader = builder.build().columnReader(0);
        builder.appendNull();
        assertThat(reader.type()).isEqualTo(ColumnType.LONG);
        for (int row = 0; row < 64; row++) {
            assertThat(reader.isNullAt(row)).isFalse();
            assertThat(reader.longValue(row)).isEqualTo(row);
        }
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> reader.isNullAt(64))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void packedNullsCrossWordBoundariesAndRemainImmutableWhenBuilderGrows() {
        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(65)
                .column("n", ColumnType.LONG).column("s", ColumnType.STRING);
        for (int row = 0; row < 65; row++) {
            if (row == 0 || row == 63 || row == 64) builder.appendNull(); else builder.appendLong(row);
            builder.appendString(row == 31 ? null : "value" + row);
        }
        ResultFrame snapshot = builder.build();
        for (int row = 65; row < 200; row++) {
            if (row % 7 == 0) builder.appendNull(); else builder.appendLong(row);
            builder.appendString(row % 9 == 0 ? null : "value" + row);
        }
        ResultFrame grown = builder.build();
        assertThat(snapshot.rowCount()).isEqualTo(65);
        for (int row = 0; row < 200; row++) {
            boolean missingNumber = row < 65 ? row == 0 || row == 63 || row == 64 : row % 7 == 0;
            assertThat(grown.isNullAt(row, 0)).isEqualTo(missingNumber);
            if (!missingNumber) assertThat(grown.longAt(row, 0)).isEqualTo(row);
            boolean missingString = row < 65 ? row == 31 : row % 9 == 0;
            assertThat(grown.isNullAt(row, 1)).isEqualTo(missingString);
            if (row < 65) assertThat(snapshot.rows().get(row)).isEqualTo(grown.rows().get(row));
        }
    }

    @Test
    void lateFirstNullDoesNotChangeAnEarlierAllValidSnapshot() {
        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(64).column("n", ColumnType.LONG);
        for (int row = 0; row < 64; row++) builder.appendLong(row);
        ResultFrame beforeNull = builder.build();
        builder.appendNull();
        ResultFrame afterNull = builder.build();
        assertThat(afterNull.isNullAt(64, 0)).isTrue();
        for (int row = 0; row < 64; row++) {
            assertThat(beforeNull.isNullAt(row, 0)).isFalse();
            assertThat(afterNull.longAt(row, 0)).isEqualTo(row);
        }
    }

    @Test
    void exactlySizedBuildSharesSafelyAndDetachesBeforeBuilderGrowth() {
        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(32).column("value", ColumnType.LONG);
        for (int row = 0; row < 32; row++) builder.appendLong(row);
        ResultFrame first = builder.build(), second = builder.build();
        builder.appendLong(32);
        assertThat(first.rowCount()).isEqualTo(32);
        assertThat(second.rows()).isEqualTo(first.rows());
        assertThat(first.longAt(31, 0)).isEqualTo(31);
        assertThat(builder.build().longAt(32, 0)).isEqualTo(32);
    }

    @Test
    void capacityHintAndTypedCellCopyPreserveNullsExactNumbersAndSnapshots() {
        ResultFrame source = ResultFrame.builder().expectedRows(1).column("large", ColumnType.LONG)
                .column("decimal", ColumnType.DECIMAL).column("missing", ColumnType.STRING)
                .row(Long.MAX_VALUE, new java.math.BigDecimal("1.2300"), null).build();
        ResultFrame.Builder copy = ResultFrame.builder().expectedRows(1).column("large", ColumnType.LONG)
                .column("decimal", ColumnType.DECIMAL).column("missing", ColumnType.STRING);
        for (int column = 0; column < 3; column++) copy.appendCell(source, 0, column);
        ResultFrame snapshot = copy.build();
        for (int row = 0; row < 40; row++) for (int column = 0; column < 3; column++) copy.appendCell(source, 0, column);
        assertThat(snapshot.rows()).isEqualTo(source.rows());
        assertThat(copy.build().rowCount()).isEqualTo(41);
        assertThat(snapshot.columnTypeAt(1)).isEqualTo(ColumnType.DECIMAL);
    }

    @Test
    void builderSnapshotDoesNotChangeWhenBuilderIsReused() {
        ResultFrame.Builder builder = ResultFrame.builder()
                .column("value", ColumnType.DOUBLE)
                .row(Map.of("value", 1.0));

        ResultFrame frame = builder.build();
        builder.row(Map.of("value", 2.0));

        assertThat(frame.rowCount()).isEqualTo(1);
        assertThat(frame.rows().get(0).get("value")).isEqualTo(1.0);
    }

    @Test
    void rowsReturnsTheCachedReadOnlyView() {
        ResultFrame frame = ResultFrame.builder()
                .column("value", ColumnType.DOUBLE)
                .row(Map.of("value", 1.0))
                .build();

        assertThat(frame.rows()).isSameAs(frame.rows());
    }

    @Test
    void typedColumnsPreserveNullsWithoutBoxedRowsOnTheWritePath() {
        ResultFrame frame = ResultFrame.builder()
                .column("bucket", ColumnType.LONG)
                .column("value", ColumnType.DOUBLE)
                .column("service", ColumnType.STRING)
                .appendLong(1_700_000_000L)
                .appendNull()
                .appendString("api")
                .build();

        assertThat(frame.longAt(0, frame.columnIndex("bucket"))).isEqualTo(1_700_000_000L);
        assertThat(frame.isNullAt(0, frame.columnIndex("value"))).isTrue();
        assertThat(frame.stringAt(0, frame.columnIndex("service"))).isEqualTo("api");
        assertThat(frame.rows().get(0)).containsEntry("value", null).containsEntry("service", "api");
    }
}
