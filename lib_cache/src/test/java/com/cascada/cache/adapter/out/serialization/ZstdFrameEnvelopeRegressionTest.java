package com.cascada.cache.adapter.out.serialization;

import com.github.luben.zstd.Zstd;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZstdFrameEnvelopeRegressionTest {
    @Test
    void checksumDetectsPayloadChangesThatDoNotChangeTheDecodedLength() {
        byte[] source = new byte[4096];
        new java.util.Random(7).nextBytes(source);
        byte[] compressed = ZstdFrameEnvelope.compress(source, source.length, 3);
        assertThat(ZstdFrameEnvelope.decompress(compressed)).containsExactly(source);
        compressed[compressed.length / 2] ^= 1;
        assertThatThrownBy(() -> ZstdFrameEnvelope.decompress(compressed))
                .isInstanceOf(com.github.luben.zstd.ZstdException.class).hasMessageContaining("checksum");
    }

    @Test
    void oldBlobsWithoutChecksumsRemainReadable() {
        byte[] source = "legacy cache data".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] legacyZstd = Zstd.compress(source);
        byte[] envelope = ByteBuffer.allocate(4 + legacyZstd.length).putInt(source.length).put(legacyZstd).array();
        assertThat(ZstdFrameEnvelope.decompress(envelope)).containsExactly(source);
    }
}
