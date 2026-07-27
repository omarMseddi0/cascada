package com.cascada.cache.domain.frame;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ResultFrameTest {

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
}
