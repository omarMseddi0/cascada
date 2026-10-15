package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CacheSerializationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryBlobCacheBackendAdapterTest {

    private static final String KEY = "QC:V4:B86400:abc:0";

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.ofEpochSecond(1_000));
    private final Clock movableClock = new Clock() {
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now.get(); }
    };

    private ResultFrame oneRow() {
        return ResultFrame.builder().column("n", ColumnType.LONG).row(1L).build();
    }

    @Test
    void aStoredBucketDisappearsWhenItsTimeToLiveElapses() {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer(), movableClock);
        backend.store(KEY, oneRow(), Duration.ofSeconds(60));

        assertThat(backend.existsForKeys(List.of(KEY))).containsExactly(true);
        now.set(now.get().plusSeconds(60));
        assertThat(backend.existsForKeys(List.of(KEY))).containsExactly(false);
        assertThat(backend.multiGet(List.of(KEY))).containsExactly(Optional.empty());
    }

    @Test
    void anUndecodableBucketIsReportedMissingAndEvicted() {
        CacheValueSerializerPort corruptOnRead = new CacheValueSerializerPort() {
            @Override public byte[] serialize(ResultFrame frame) { return new byte[] {1}; }
            @Override public ResultFrame deserialize(byte[] blob) {
                throw new CacheSerializationException("corrupt", new IllegalStateException());
            }
        };
        var backend = new InMemoryBlobCacheBackendAdapter(corruptOnRead, movableClock);
        backend.store(KEY, oneRow(), Duration.ofDays(1));

        assertThat(backend.multiGet(List.of(KEY))).containsExactly(Optional.empty());
        assertThat(backend.existsForKeys(List.of(KEY))).containsExactly(false);
    }
}
