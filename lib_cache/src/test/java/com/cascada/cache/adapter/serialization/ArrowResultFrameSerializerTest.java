package com.cascada.cache.adapter.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheValueSerializerPort;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the Arrow IPC + zstd cache serializer round-trips a {@link ResultFrame} losslessly and is a
 * true Liskov substitute for {@link PortableFrameSerializer}: the same logical frame in yields the same
 * logical frame out of either serializer, so the cache behaves identically whichever is injected.
 * Covers all three column types, nulls, the empty frame, and corruption handling.
 */
class ArrowResultFrameSerializerTest {

    private final ArrowResultFrameSerializer arrow = new ArrowResultFrameSerializer();
    private final PortableFrameSerializer portable = new PortableFrameSerializer();

    private ResultFrame sampleFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("COUNT(latency)", ColumnType.LONG)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .row(row("netflix", 4L, 100.5))
                .row(row("youtube", 2L, 20.25))
                .row(rowWithNulls("hbo"))
                .build();
    }

    private Map<String, Object> row(String app, long count, double sum) {
        Map<String, Object> values = new HashMap<>();
        values.put("appName", app);
        values.put("COUNT(latency)", count);
        values.put("SUM(latency)", sum);
        return values;
    }

    private Map<String, Object> rowWithNulls(String app) {
        Map<String, Object> values = new HashMap<>();
        values.put("appName", app);
        values.put("COUNT(latency)", null);
        values.put("SUM(latency)", null);
        return values;
    }

    @Test
    void roundTripsAFrameLosslessly() {
        ResultFrame restored = arrow.deserialize(arrow.serialize(sampleFrame()));

        assertThat(restored.columnNames()).containsExactly("appName", "COUNT(latency)", "SUM(latency)");
        assertThat(restored.columnTypes())
                .containsEntry("appName", ColumnType.STRING)
                .containsEntry("COUNT(latency)", ColumnType.LONG)
                .containsEntry("SUM(latency)", ColumnType.DOUBLE);
        assertThat(restored.rowCount()).isEqualTo(3);
        assertThat(restored.rows().get(0).get("appName")).isEqualTo("netflix");
        assertThat(((Number) restored.rows().get(0).get("COUNT(latency)")).longValue()).isEqualTo(4L);
        assertThat(((Number) restored.rows().get(1).get("SUM(latency)")).doubleValue()).isEqualTo(20.25);
        // Null cells survive as nulls.
        assertThat(restored.rows().get(2).get("COUNT(latency)")).isNull();
        assertThat(restored.rows().get(2).get("SUM(latency)")).isNull();
        assertThat(restored.rows().get(2).get("appName")).isEqualTo("hbo");
    }

    @Test
    void roundTripsTheEmptyFrame() {
        ResultFrame restored = arrow.deserialize(arrow.serialize(ResultFrame.empty()));
        assertThat(restored.isEmpty()).isTrue();
        assertThat(restored.columnNames()).isEmpty();
    }

    @Test
    void isLiskovSubstitutableWithThePortableSerializer() {
        // A frame restored through Arrow is indistinguishable from one restored through the portable
        // codec: same schema, same row count, same cell values. Either serializer is interchangeable.
        ResultFrame viaArrow = arrow.deserialize(arrow.serialize(sampleFrame()));
        ResultFrame viaPortable = portable.deserialize(portable.serialize(sampleFrame()));

        assertThat(viaArrow.columnNames()).isEqualTo(viaPortable.columnNames());
        assertThat(viaArrow.columnTypes()).isEqualTo(viaPortable.columnTypes());
        assertThat(viaArrow.rowCount()).isEqualTo(viaPortable.rowCount());
        for (int rowIndex = 0; rowIndex < viaArrow.rowCount(); rowIndex++) {
            for (String column : viaArrow.columnNames()) {
                Object a = viaArrow.rows().get(rowIndex).get(column);
                Object p = viaPortable.rows().get(rowIndex).get(column);
                if (a instanceof Number an && p instanceof Number pn) {
                    assertThat(an.doubleValue()).isEqualTo(pn.doubleValue());
                } else {
                    assertThat(a).isEqualTo(p);
                }
            }
        }
    }

    @Test
    void rejectsACorruptBlob() {
        byte[] blob = arrow.serialize(sampleFrame());
        blob[blob.length - 1] ^= 0x7F; // flip bits in the compressed body
        assertThatThrownBy(() -> arrow.deserialize(blob))
                .isInstanceOf(CacheValueSerializerPort.CacheSerializationException.class);
    }
}
