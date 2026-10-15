package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Verifies global cache measurement, full flush, and surgical prefix flush behavior. */
final class CacheAdministrationTest {

    private final CacheBackendPort backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
    private final CacheAdministrationService admin = new CacheAdministrationService(backend);

    private ResultFrame frame(int rows) {
        ResultFrame.Builder builder = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("SUM(bytes)", ColumnType.DOUBLE);
        for (int i = 0; i < rows; i++) {
            builder.row(Map.of("appName", "app" + i, "SUM(bytes)", (double) i));
        }
        return builder.build();
    }

    private String bucketKey(String hash, long bucketStart) {
        return "QC:V4:B86400:" + hash + ":" + bucketStart;
    }

    @Test
    void emptyCacheReportsZeroMegabytesAndBuckets() {
        CacheSizeReport report = admin.measureCacheSize();
        assertThat(report.totalBytes()).isZero();
        assertThat(report.totalMegabytes()).isZero();
        assertThat(report.bucketCount()).isZero();
    }

    @Test
    void sizeReportMeasuresAllStoredBucketBlobsGlobally() {
        backend.store(bucketKey("abc123", 0), frame(100));
        backend.store(bucketKey("abc123", 86_400), frame(100));

        CacheSizeReport report = admin.measureCacheSize();

        assertThat(report.bucketCount()).isEqualTo(2);
        assertThat(report.totalBytes()).isGreaterThan(0);
        assertThat(report.totalMegabytes())
                .isEqualTo(Math.round(report.totalBytes() / (1024.0 * 1024.0) * 100.0) / 100.0);
    }

    @Test
    void globalMeasurementAndFlushIgnoreKeysOutsideTheBucketNamespace() {
        InMemoryBlobCacheBackendAdapter sharedBackend =
                new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        CacheAdministrationService sharedAdmin = new CacheAdministrationService(sharedBackend);
        sharedBackend.store(bucketKey("hash", 0), frame(10));
        sharedBackend.store("application:settings:theme", frame(1));

        assertThat(sharedAdmin.measureCacheSize().bucketCount()).isEqualTo(1);
        assertThat(sharedAdmin.flushEverything()).isEqualTo(1);

        assertThat(sharedBackend.storedBucketCount()).isEqualTo(1);
        assertThat(sharedAdmin.measureCacheSize()).isEqualTo(CacheSizeReport.empty());
    }

    @Test
    void flushEverythingClearsCoverageAndCubeState() {
        InMemoryCoverageIndexAdapter coverage = new InMemoryCoverageIndexAdapter();
        CubeShapeCatalog cube = new CubeShapeCatalog();
        CacheAdministrationService invalidatingAdmin = new CacheAdministrationService(backend, coverage, cube);
        QueryHash hash = QueryHash.of("0000000000000000000000000000000a");
        coverage.markCached(hash, 86_400, 0);
        cube.register(new TimeRange(0, 86_399),
                new QueryShape(java.util.Set.of(), java.util.Set.of(), java.util.Set.of("SUM(x)")),
                ResultFrame.builder().column("SUM(x)", ColumnType.DOUBLE).row(Map.of("SUM(x)", 1.0)).build());

        invalidatingAdmin.flushEverything();

        assertThat(coverage.load(hash, 86_400)).isEmpty();
        assertThat(cube.windowCount()).isZero();
    }

    @Test
    void flushKeyPrefixEvictsOnlyTheSelectedQueryFamily() {
        backend.store(bucketKey("hashA", 0), frame(10));
        backend.store(bucketKey("hashA", 86_400), frame(10));
        backend.store(bucketKey("hashB", 0), frame(10));

        long purged = admin.flushKeyPrefix("QC:V4:B86400:hashA:");

        assertThat(purged).isEqualTo(2);
        assertThat(admin.measureCacheSize().bucketCount()).isEqualTo(1);
    }

    @Test
    void everythingScopeMatchesOnlyWellFormedCacheBucketKeys() {
        assertThat(CacheScope.everything().matches("QC:V4:B86400:hash:0")).isTrue();
        assertThat(CacheScope.everything().matches("QC:V4:B86400:hash:0:extra")).isFalse();
        assertThat(CacheScope.everything().matches("application:settings:theme")).isFalse();
        assertThat(CacheScope.forKeyPrefix("QC:V4:B86400:hash:").matches("QC:V4:B86400:hash:0")).isTrue();
        assertThat(CacheScope.forKeyPrefix("QC:V4:B86400:hash:").matches("QC:V4:B86400:other:0")).isFalse();
        assertThat(CacheKeyConstants.isBucketKey("QC:V4:badBucket:hash:0")).isFalse();
    }
}
