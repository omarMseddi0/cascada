package com.cascada.cache.admin;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.application.CacheAdministrationService;
import com.cascada.cache.domain.admin.CacheKeyTenantSegment;
import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.identity.domain.TenantIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the open-source admin features (plan §8.17, system card §10): the "cache size" button reports
 * the real stored blob bytes in megabytes with a correct per-tenant breakdown, and "flush" purges the
 * right scope and only that scope. Runs against the in-memory backend, which stores the same blobs a
 * Valkey tier would, so the measured bytes are representative.
 */
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

    private String tenantBucketKey(String tenant, long bucketStart) {
        // mirrors the production key shape: <tenantSegment>:QC:V4:B86400:<hash>:<ts>
        return tenant + ":QC:V4:B86400:abc123:" + bucketStart;
    }

    @Test
    void emptyCacheReportsZeroMegabytes() {
        CacheSizeReport report = admin.measureCacheSize();
        assertThat(report.totalBytes()).isZero();
        assertThat(report.totalMegabytes()).isZero();
        assertThat(report.bucketCount()).isZero();
        assertThat(report.bytesByTenant()).isEmpty();
    }

    @Test
    void sizeReportSumsStoredBlobBytesAndCountsBuckets() {
        backend.store(tenantBucketKey("acme", 0), frame(100));
        backend.store(tenantBucketKey("acme", 86_400), frame(100));

        CacheSizeReport report = admin.measureCacheSize();

        assertThat(report.bucketCount()).isEqualTo(2);
        assertThat(report.totalBytes()).isGreaterThan(0);
        // the headline MB figure is derived from the byte total
        assertThat(report.totalMegabytes())
                .isEqualTo(Math.round(report.totalBytes() / (1024.0 * 1024.0) * 100.0) / 100.0);
    }

    @Test
    void sizeReportBreaksDownBytesByTenant() {
        backend.store(tenantBucketKey("acme", 0), frame(200));
        backend.store(tenantBucketKey("acme", 86_400), frame(200));
        backend.store(tenantBucketKey("globex", 0), frame(50));

        CacheSizeReport report = admin.measureCacheSize();

        assertThat(report.bytesByTenant()).containsKeys("acme", "globex");
        // acme stored more rows in more buckets, so it must account for more bytes than globex
        assertThat(report.bytesByTenant().get("acme")).isGreaterThan(report.bytesByTenant().get("globex"));
        long sumOfParts = report.bytesByTenant().values().stream().mapToLong(Long::longValue).sum();
        assertThat(sumOfParts).isEqualTo(report.totalBytes());
    }

    @Test
    void perTenantMeasurementReconcilesWithGlobalTotal() {
        backend.store(tenantBucketKey("acme", 0), frame(120));
        backend.store(tenantBucketKey("globex", 0), frame(30));

        long acmeBytes = admin.measureCacheSize(TenantIdentifier.of("acme")).totalBytes();
        long globexBytes = admin.measureCacheSize(TenantIdentifier.of("globex")).totalBytes();

        assertThat(acmeBytes + globexBytes).isEqualTo(admin.measureCacheSize().totalBytes());
        assertThat(admin.measureCacheSize(TenantIdentifier.of("absent")).totalBytes()).isZero();
    }

    @Test
    void flushTenantPurgesOnlyThatTenant() {
        backend.store(tenantBucketKey("acme", 0), frame(10));
        backend.store(tenantBucketKey("acme", 86_400), frame(10));
        backend.store(tenantBucketKey("globex", 0), frame(10));

        long purged = admin.flushTenant(TenantIdentifier.of("acme"));

        assertThat(purged).isEqualTo(2);
        CacheSizeReport after = admin.measureCacheSize();
        assertThat(after.bucketCount()).isEqualTo(1);
        assertThat(after.bytesByTenant()).containsOnlyKeys("globex");
    }

    @Test
    void flushEverythingEmptiesTheCache() {
        backend.store(tenantBucketKey("acme", 0), frame(10));
        backend.store(tenantBucketKey("globex", 0), frame(10));

        long purged = admin.flushEverything();

        assertThat(purged).isEqualTo(2);
        assertThat(admin.measureCacheSize().totalBytes()).isZero();
    }

    @Test
    void flushKeyPrefixEvictsSurgically() {
        backend.store("acme:QC:V4:B86400:hashA:0", frame(10));
        backend.store("acme:QC:V4:B86400:hashB:0", frame(10));

        long purged = admin.flushKeyPrefix("acme:QC:V4:B86400:hashA:");

        assertThat(purged).isEqualTo(1);
        assertThat(admin.measureCacheSize().bucketCount()).isEqualTo(1);
    }

    @Test
    void bareBucketKeyWithoutTenantPrefixIsAttributedToDefault() {
        assertThat(CacheKeyTenantSegment.of("QC:V4:B86400:hash:0"))
                .isEqualTo(CacheKeyTenantSegment.DEFAULT_TENANT);
        assertThat(CacheKeyTenantSegment.of("acme:QC:V4:B86400:hash:0")).isEqualTo("acme");
    }

    @Test
    void everythingScopeMatchesAnyKey() {
        assertThat(CacheScope.everything().matches("anything")).isTrue();
        assertThat(CacheScope.forKeyPrefix("acme:").matches("acme:QC:V4")).isTrue();
        assertThat(CacheScope.forKeyPrefix("acme:").matches("globex:QC:V4")).isFalse();
    }
}
