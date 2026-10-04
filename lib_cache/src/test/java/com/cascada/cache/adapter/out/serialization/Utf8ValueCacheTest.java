package com.cascada.cache.adapter.out.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class Utf8ValueCacheTest {
    @Test
    void repeatedUtf8ValuesShareStringsWithoutChangingValuesNullsOrOrder() {
        List<String> values = List.of("", "東京😀", "Aa", "BB", "é", "e\u0301", "abcdefgh-X", "abcdefgh-Y", "\u0000");
        ResultFrame.Builder builder = ResultFrame.builder().column("s", ColumnType.STRING);
        for (int row = 0; row < 2_000; row++) builder.appendString(row % 31 == 0 ? null : values.get(row % values.size()));
        ResultFrame original = builder.build();
        ArrowResultFrameSerializer codec = new ArrowResultFrameSerializer();
        ResultFrame decoded = codec.deserialize(codec.serialize(original));
        assertThat(decoded.rows()).isEqualTo(original.rows());
        // Equal byte sequences from different rows reuse immutable String values.
        assertThat(decoded.stringAt(1, 0)).isSameAs(decoded.stringAt(10, 0));
        assertThat(decoded.stringAt(2, 0)).isNotEqualTo(decoded.stringAt(3, 0));
    }

    @Test
    void highCardinalityAndOversizeValuesRemainLosslessAfterTheCacheFallsBack() {
        ResultFrame.Builder builder = ResultFrame.builder().expectedRows(5_002).column("s", ColumnType.STRING);
        for (int row = 0; row < 5_000; row++) builder.appendString("distinct-東京-" + row);
        builder.appendString("x".repeat(1_100_000));
        builder.appendString("distinct-東京-0");
        ResultFrame original = builder.build();
        ArrowResultFrameSerializer codec = new ArrowResultFrameSerializer();
        assertThat(codec.deserialize(codec.serialize(original)).rows()).isEqualTo(original.rows());
    }

    @Test
    void dictionaryRemainsCorrectAcrossArrowVectorBufferReuse() {
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var vector = new org.apache.arrow.vector.VarCharVector("s", allocator)) {
            Utf8ValueCache cache = new Utf8ValueCache();
            vector.allocateNew();
            vector.setSafe(0, "abcdefgh-東京".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            String first = cache.read(vector, 0);
            vector.clear();
            vector.allocateNew();
            vector.setSafe(0, "abcdefgh-different".getBytes(StandardCharsets.UTF_8));
            vector.setSafe(1, "abcdefgh-東京".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(2);
            assertThat(cache.read(vector, 0)).isEqualTo("abcdefgh-different");
            assertThat(cache.read(vector, 1)).isSameAs(first);
        }
    }

    @Test
    void rejectsInvalidOffsetsBeforeCreatingANativeLookupView() {
        try (var allocator = new org.apache.arrow.memory.RootAllocator();
             var vector = new org.apache.arrow.vector.VarCharVector("s", allocator)) {
            vector.allocateNew();
            vector.setSafe(0, "value".getBytes(StandardCharsets.UTF_8));
            vector.setValueCount(1);
            var offsets = vector.getOffsetBuffer();
            offsets.setInt(0, -1);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new Utf8ValueCache().read(vector,0))
                    .isInstanceOf(IllegalArgumentException.class);
            offsets.setInt(0, 0); offsets.setInt(4, Integer.MAX_VALUE);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> new Utf8ValueCache().read(vector,0))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
