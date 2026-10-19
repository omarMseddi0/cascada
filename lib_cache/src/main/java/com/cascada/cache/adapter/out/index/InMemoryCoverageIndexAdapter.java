package com.cascada.cache.adapter.out.index;

import com.cascada.cache.adapter.out.index.model.CoverageEntry;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.domain.index.BucketCoverageBitmap;
import com.cascada.identity.domain.model.QueryHash;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/** Bounded advisory index. Evicting a family only returns its reader to pipelined backend EXISTS. */
public final class InMemoryCoverageIndexAdapter implements CoverageIndexPort {
    private static final long MAX_FAMILY_BYTES = 1024L * 1024;
    private final Map<String, CoverageEntry> families = new LinkedHashMap<>(16, 0.75f, true);
    private final int maximumFamilies;
    private final long maximumBytes;
    private long retainedBytes;

    public InMemoryCoverageIndexAdapter() { this(10_000, 64L * 1024 * 1024); }
    public InMemoryCoverageIndexAdapter(int maximumFamilies, long maximumBytes) {
        if (maximumFamilies <= 0 || maximumBytes <= 0) throw new IllegalArgumentException("positive index bounds required");
        this.maximumFamilies = maximumFamilies;
        this.maximumBytes = maximumBytes;
    }

    @Override public synchronized Optional<BucketCoverageBitmap> load(QueryHash hash, long seconds) {
        CoverageEntry entry = families.get(key(hash, seconds));
        return entry == null ? Optional.empty() : Optional.of(entry.bitmap().copy());
    }
    @Override public synchronized void markCached(QueryHash hash, long seconds, long start) {
        if (seconds <= 0) throw new IllegalArgumentException("bucket width must be positive");
        long ordinal = Math.floorDiv(start, seconds);
        if (ordinal < 0 || ordinal > Integer.MAX_VALUE) return;
        long bytes = (ordinal / 64 + 1) * Long.BYTES + 128;
        if (bytes > MAX_FAMILY_BYTES || bytes > maximumBytes) return;
        String key = key(hash, seconds);
        CoverageEntry previous = families.remove(key);
        if (previous != null) retainedBytes -= previous.bytes();
        bytes = previous == null ? bytes : Math.max(bytes, previous.bytes());
        while (!families.isEmpty() && (families.size() >= maximumFamilies || retainedBytes + bytes > maximumBytes)) {
            var iterator = families.entrySet().iterator();
            retainedBytes -= iterator.next().getValue().bytes();
            iterator.remove();
        }
        BucketCoverageBitmap bitmap = previous == null ? new BucketCoverageBitmap(seconds) : previous.bitmap();
        bitmap.cover(start);
        families.put(key, new CoverageEntry(bitmap, bytes));
        retainedBytes += bytes;
    }
    @Override public synchronized void markEvicted(QueryHash hash, long seconds, long start) {
        CoverageEntry entry = families.get(key(hash, seconds));
        if (entry != null) entry.bitmap().uncover(start);
    }
    @Override public synchronized void clear() { families.clear(); retainedBytes = 0; }
    public synchronized int indexedFamilyCount() { return families.size(); }
    public synchronized long estimatedRetainedBytes() { return retainedBytes; }
    private static String key(QueryHash hash, long seconds) { return "CV:B" + seconds + ":" + hash.value(); }
}
