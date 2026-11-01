package com.cascada.cache.domain.frame;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ParallelFrameBuilderTest {
    @Test
    void disjointWritersPreserveNullBitsTypesAndTheSealedSnapshot() {
        ResultFrame schema = ResultFrame.builder().column("n", ColumnType.LONG)
                .column("d", ColumnType.DOUBLE).column("s", ColumnType.STRING)
                .column("money", ColumnType.DECIMAL).build();
        ParallelFrameBuilder output = new ParallelFrameBuilder(schema, 8_003, 10);
        try (var executor = Executors.newFixedThreadPool(3)) {
            CompletableFuture<?>[] tasks = new CompletableFuture<?>[output.writerCount()];
            for (int index = 0; index < tasks.length; index++) {
                ResultFrameBuilder writer = output.writer(index);
                int start = output.startRow(index), end = output.endRow(index);
                assertThat(start % 64).isZero();
                tasks[index] = CompletableFuture.runAsync(() -> {
                    for (int row = start; row < end; row++) {
                        if (row % 7 == 0) writer.appendNull(); else writer.appendLong(row);
                        writer.appendDouble(row + 0.5);
                        writer.appendString(row % 11 == 0 ? null : "s" + row);
                        writer.appendDecimal(row % 13 == 0 ? null : BigDecimal.valueOf(row, 2));
                    }
                }, executor);
            }
            CompletableFuture.allOf(tasks).join();
        }
        ResultFrame result = output.build();
        assertThat(output.build()).isSameAs(result);
        for (int row = 0; row < result.rowCount(); row++) {
            assertThat(result.isNullAt(row, 0)).isEqualTo(row % 7 == 0);
            if (row % 7 != 0) assertThat(result.longAt(row, 0)).isEqualTo(row);
            assertThat(result.doubleAt(row, 1)).isEqualTo(row + 0.5);
            assertThat(result.valueAt(row, 2)).isEqualTo(row % 11 == 0 ? null : "s" + row);
            assertThat(result.valueAt(row, 3)).isEqualTo(row % 13 == 0 ? null : BigDecimal.valueOf(row, 2));
        }
        assertThatThrownBy(() -> output.writer(0).appendLong(999)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void incompleteOrOverflowingRangesCannotPublishOrOverwriteOtherRows() {
        ResultFrame schema = ResultFrame.builder().column("n", ColumnType.LONG).build();
        ParallelFrameBuilder output = new ParallelFrameBuilder(schema, 65, 2);
        for (int row = 0; row < 64; row++) output.writer(0).appendLong(row);
        assertThatThrownBy(output::build).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> output.writer(0).appendLong(1)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(output.writer(0)::build).isInstanceOf(IllegalStateException.class);
        output.writer(1).appendNull();
        ResultFrame result = output.build();
        assertThat(result.rowCount()).isEqualTo(65);
        assertThat(result.longAt(63, 0)).isEqualTo(63);
        assertThat(result.isNullAt(64, 0)).isTrue();
    }

    @Test
    void emptyFrameRetainsSchemaWithoutStartingWriters() {
        ResultFrame schema = ResultFrame.builder().column("n", ColumnType.LONG).build();
        ParallelFrameBuilder output = new ParallelFrameBuilder(schema, 0, 10);
        assertThat(output.writerCount()).isZero();
        assertThat(output.build().rowCount()).isZero();
        assertThat(output.build().columnNames()).containsExactly("n");
    }
}
