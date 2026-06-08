package com.cascada.cache.adapter.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheValueSerializerPort;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PortableFrameSerializerTest {

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();

    private ResultFrame sampleFrame() {
        return ResultFrame.builder()
                .column("ts", ColumnType.LONG)
                .column("appName", ColumnType.STRING)
                .column("sumBytes", ColumnType.DOUBLE)
                .row(Map.of("ts", 86_400L, "appName", "netflix", "sumBytes", 12.5))
                .row(Map.of("ts", 86_700L, "appName", "youtube", "sumBytes", 7.0))
                .build();
    }

    @Test
    void roundTripsAFrameLosslessly() {
        ResultFrame original = sampleFrame();
        ResultFrame restored = serializer.deserialize(serializer.serialize(original));

        assertThat(restored.columnNames()).isEqualTo(original.columnNames());
        assertThat(restored.columnTypes()).isEqualTo(original.columnTypes());
        assertThat(restored.rows()).isEqualTo(original.rows());
    }

    @Test
    void compressionActuallyShrinksARepetitiveFrame() {
        ResultFrame.Builder builder = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("sumBytes", ColumnType.DOUBLE);
        for (int index = 0; index < 1_000; index++) {
            builder.row(Map.of("appName", "netflix", "sumBytes", 1.0));
        }
        byte[] blob = serializer.serialize(builder.build());
        assertThat(blob.length).isLessThan(1_000 * 16);
    }

    @Test
    void corruptBlobRaisesSerializationException() {
        assertThatThrownBy(() -> serializer.deserialize(new byte[]{0, 0, 0, 8, 1, 2, 3}))
                .isInstanceOf(CacheValueSerializerPort.CacheSerializationException.class);
    }
}
