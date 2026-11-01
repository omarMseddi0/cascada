package com.cascada.cache.adapter.out.index;

import com.cascada.identity.domain.model.QueryHash;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class BoundedCoverageIndexTest {
    private QueryHash hash(int value) { return QueryHash.of(String.format("%032x", value)); }
    @Test void familyAndMemoryBoundsEvictOnlyAdvisoryState() {
        var index = new InMemoryCoverageIndexAdapter(2, 4000);
        long day = 20_000L * 86_400;
        index.markCached(hash(1), 86_400, day);
        index.markCached(hash(2), 86_400, day);
        assertThat(index.estimatedRetainedBytes()).isLessThanOrEqualTo(4000);
        assertThat(index.load(hash(1), 86_400)).isEmpty();
        assertThat(index.load(hash(2), 86_400)).isPresent();
        index.markCached(hash(3), 86_400, day);
        assertThat(index.indexedFamilyCount()).isLessThanOrEqualTo(2);
    }
    @Test void aFarFutureTimestampCannotAllocateAnEpochSizedBitset() {
        var index = new InMemoryCoverageIndexAdapter();
        index.markCached(hash(1), 1, Integer.MAX_VALUE);
        assertThat(index.indexedFamilyCount()).isZero();
        assertThat(index.estimatedRetainedBytes()).isZero();
    }
    @Test void snapshotsDoNotShareMutableBitsWithTheWriter() {
        var index = new InMemoryCoverageIndexAdapter();
        index.markCached(hash(1), 86_400, 0);
        var snapshot = index.load(hash(1), 86_400).orElseThrow();
        index.markEvicted(hash(1), 86_400, 0);
        assertThat(snapshot.isCovered(0)).isTrue();
        assertThat(index.load(hash(1), 86_400).orElseThrow().isCovered(0)).isFalse();
    }
}
