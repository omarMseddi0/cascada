package com.cascada.cache.adapter.out.serialization;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import com.github.luben.zstd.Zstd;

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
    void blobsRemainReadableAcrossConfiguredCompressionLevels() {
        ResultFrame original = sampleFrame();
        PortableFrameSerializer[] writers = {
                new PortableFrameSerializer(1), new PortableFrameSerializer(3), new PortableFrameSerializer(9)
        };
        PortableFrameSerializer reader = new PortableFrameSerializer(9);

        for (PortableFrameSerializer writer : writers) {
            assertThat(reader.deserialize(writer.serialize(original)).rows()).isEqualTo(original.rows());
        }
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

    @Test
    void roundTripsStringsBeyondModifiedUtfLimit() {
        String value = "é😀".repeat(30_000);
        ResultFrame frame = ResultFrame.builder().column("value", ColumnType.STRING)
                .row(Map.of("value", value)).build();
        assertThat(serializer.deserialize(serializer.serialize(frame)).rows()).isEqualTo(frame.rows());
    }

    @Test
    void readsExistingPortableEncoding() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(1);
            out.writeUTF("value");
            out.writeByte(ColumnType.STRING.ordinal());
            out.writeInt(1);
            out.writeBoolean(true);
            out.writeUTF("old cached value");
        }
        byte[] compressed = Zstd.compress(bytes.toByteArray());
        byte[] blob = ByteBuffer.allocate(4 + compressed.length).putInt(bytes.size())
                .put(compressed).array();
        assertThat(serializer.deserialize(blob).rows()).containsExactly(Map.of("value", "old cached value"));
    }

    @Test
    void rejectsForgedLengthBeforeAllocation() {
        byte[] blob = serializer.serialize(sampleFrame());
        ByteBuffer.wrap(blob).putInt(Integer.MAX_VALUE);
        assertThatThrownBy(() -> serializer.deserialize(blob))
                .isInstanceOf(CacheValueSerializerPort.CacheSerializationException.class);
        assertThatThrownBy(() -> new ArrowResultFrameSerializer().deserialize(blob))
                .isInstanceOf(CacheValueSerializerPort.CacheSerializationException.class);
    }

    @Test
    void matchesExistingModernWireBytesForEveryTypeAndExtremeValue() throws Exception {
        ResultFrame.Builder builder = ResultFrame.builder().column("l", ColumnType.LONG)
                .column("d", ColumnType.DOUBLE).column("s", ColumnType.STRING).column("n", ColumnType.DECIMAL);
        long[] longs = {Long.MIN_VALUE, Long.MAX_VALUE, 9_007_199_254_740_993L, 0};
        double[] doubles = {-0.0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
                Double.longBitsToDouble(0x7ff0000000000001L)};
        String[] strings = {"", "\u0000", "\u6771\u4eac\ud83d\ude00", "x".repeat(100_000)};
        for (int i = 0; i < longs.length; i++) {
            builder.appendLong(longs[i]).appendDouble(doubles[i]).appendString(strings[i])
                    .appendDecimal(new java.math.BigDecimal(i == 0 ? "1E+100" : "12345678901234567890.00000000100"));
        }
        for (int c = 0; c < 4; c++) builder.appendNull();
        ResultFrame original = builder.build();
        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(expected)) {
            out.writeInt(-1); out.writeInt(4);
            for (String name : original.columnNames()) {
                byte[] bytes = name.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                out.writeInt(bytes.length); out.write(bytes); out.writeByte(original.columnType(name).ordinal());
            }
            out.writeInt(original.rowCount());
            for (int row = 0; row < original.rowCount(); row++) {
                for (int c = 0; c < 4; c++) {
                    boolean present = !original.isNullAt(row, c); out.writeBoolean(present);
                    if (!present) continue;
                    switch (original.columnTypeAt(c)) {
                        case LONG -> out.writeLong(original.longAt(row,c));
                        case DOUBLE -> out.writeDouble(original.doubleAt(row,c));
                        default -> {
                            byte[] bytes = original.valueAt(row,c).toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                            out.writeInt(bytes.length); out.write(bytes);
                        }
                    }
                }
            }
        }
        byte[] blob = serializer.serialize(original);
        assertThat(ZstdFrameEnvelope.decompress(blob)).containsExactly(expected.toByteArray());
        ResultFrame decoded = serializer.deserialize(blob);
        assertThat(decoded.rows()).isEqualTo(original.rows());
        assertThat(Double.doubleToRawLongBits(decoded.doubleAt(0,1))).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void rejectsMalformedUtf8TruncationDimensionsTypesAndTrailingData() throws Exception {
        ResultFrame frame = ResultFrame.builder().column("s", ColumnType.STRING).appendString("a").build();
        byte[] valid = ZstdFrameEnvelope.decompress(serializer.serialize(frame));
        java.util.List<byte[]> corruptions = new java.util.ArrayList<>();
        for (int length = 0; length < valid.length; length++) corruptions.add(java.util.Arrays.copyOf(valid,length));
        byte[] badUtf = valid.clone(); badUtf[badUtf.length-1] = (byte) 0xff; corruptions.add(badUtf);
        byte[] badType = valid.clone(); badType[13] = (byte) 127; corruptions.add(badType);
        byte[] badColumns = valid.clone(); ByteBuffer.wrap(badColumns).putInt(4,Integer.MAX_VALUE); corruptions.add(badColumns);
        byte[] badRows = valid.clone(); ByteBuffer.wrap(badRows).putInt(14,Integer.MAX_VALUE); corruptions.add(badRows);
        corruptions.add(java.util.Arrays.copyOf(valid,valid.length+1));
        for (byte[] corrupted : corruptions) {
            byte[] blob = ZstdFrameEnvelope.compress(corrupted,corrupted.length,3);
            assertThatThrownBy(() -> serializer.deserialize(blob))
                    .isInstanceOf(CacheValueSerializerPort.CacheSerializationException.class);
        }
    }

    @Test
    void sharedSerializersRemainSafeUnderConcurrentCalls() throws Exception {
        ResultFrame frame = sampleFrame();
        var executor = java.util.concurrent.Executors.newFixedThreadPool(8);
        try {
            var tasks = new java.util.ArrayList<java.util.concurrent.Callable<Boolean>>();
            for (int i = 0; i < 64; i++) tasks.add(() -> serializer.deserialize(serializer.serialize(frame)).rows().equals(frame.rows()));
            for (var result : executor.invokeAll(tasks)) assertThat(result.get()).isTrue();
        } finally { executor.shutdownNow(); }
    }
}
