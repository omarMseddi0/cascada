package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.model.config.WarmingConfiguration;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.application.service.warming.DataChangeDetector;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.merge.*;
import com.cascada.cache.domain.query.*;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.safety.*;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.identity.domain.model.QueryHash;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class CacheParityRegressionTest {
    private static final long DAY = 86400;
    private static final QueryHash HASH = QueryHash.of("000000000000000000000000000000aa");
    private static ResultFrame total(long value) {
        return ResultFrame.builder().column("SUM(bytes)", ColumnType.LONG).row(value).build();
    }
    private static CanonicalQueryObject query(QueryMetadata metadata) {
        return new CanonicalQueryObject(HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()),
                new TimeRange(0, 4 * DAY - 1), PostProcessing.none(), metadata, "bucket",
                List.of("traffic"), List.of());
    }

    @Test void failedForcedRefreshDoesNotAcknowledgeTheNewBaselineAndRetriesExistingOldKeys() {
        Clock clock = Clock.fixed(Instant.ofEpochSecond(4 * DAY + 3600), ZoneOffset.UTC);
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer(), clock);
        AtomicLong source = new AtomicLong(100);
        var detector = new DataChangeDetector(backend, sql -> sql, sql -> total(source.get()),
                "validation {start} {end}", Duration.ofDays(7));
        detector.hasHistoricalDataChanged(new TimeRange(DAY, 3 * DAY - 1));
        var oldBaseline = backend.loadValidationBaseline();
        for (long day = 0; day < 4; day++) backend.store(CacheKeyFactory.buildBucketKey(HASH, day * DAY, DAY),
                total(100), Duration.ofDays(7));
        source.set(200);
        AtomicBoolean failOnce = new AtomicBoolean(true);
        var warmer = new WarmingOrchestrator(backend, sql -> {
            if (failOnce.getAndSet(false)) throw new IllegalStateException("transient Spark failure");
            return total(source.get());
        }, (sql, gap) -> sql, new WarmingQueue(), new QueryPopularityTracker(), null,
                sql -> query(QueryMetadata.globalAggregate()), detector, null,
                new WarmingConfiguration(DAY, 10, 100, Duration.ofDays(7), 100), clock);
        warmer.recordQuery(HASH, query(QueryMetadata.globalAggregate()));
        warmer.warmLookbackWindow(clock.instant().getEpochSecond(), Duration.ofDays(4));
        assertThat(backend.loadValidationBaseline()).isEqualTo(oldBaseline);
        warmer.warmLookbackWindow(clock.instant().getEpochSecond(), Duration.ofDays(4));
        for (long day = 0; day < 4; day++) {
            ResultFrame frame = backend.multiGet(List.of(CacheKeyFactory.buildBucketKey(HASH, day * DAY, DAY))).get(0).orElseThrow();
            assertThat(frame.longAt(0, 0)).isEqualTo(200);
        }
        assertThat(backend.loadValidationBaseline()).isNotEqualTo(oldBaseline);
    }

    @Test void emptyAverageIngredientsBecomeTheRequestedPublicSchemaIncludingDecimalType() {
        for (ColumnType sumType : List.of(ColumnType.LONG, ColumnType.DECIMAL)) {
            var metadata = QueryMetadata.globalAggregate().withMeasureAggregates(Map.of("avg_x", AggregateFunction.AVERAGE));
            var frame = ResultFrame.builder().column("app", ColumnType.STRING)
                    .column(AverageIngredients.sumColumn("avg_x"), sumType)
                    .column(AverageIngredients.countColumn("avg_x"), ColumnType.LONG)
                    .column(AverageIngredients.typeColumn("avg_x"), ColumnType.STRING).build();
            var merge = new FrameMergeService(300, "ts").incremental(query(metadata));
            merge.add(frame);
            ResultFrame answer = merge.finish();
            assertThat(answer.columnNames()).containsExactly("app", "avg_x");
            assertThat(answer.columnType("avg_x")).isEqualTo(sumType == ColumnType.DECIMAL ? ColumnType.DECIMAL : ColumnType.DOUBLE);
            assertThat(answer.rowCount()).isZero();
        }
    }

    @Test void viewOnlySourceBypassesTheCacheSafetyGate() {
        assertThat(SafetyRuleRegistry.defaultRegistry().findAllBypassReasons(
                query(QueryMetadata.globalAggregate().withUsesHotBatch(false)), CacheConfiguration.defaults()))
                .contains(BypassReason.HOT_VIEW_ONLY);
    }

    @Test void cubeAgeDoesNotSlideOnReads() {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.EPOCH);
        Clock clock = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return now.get(); }
        };
        var catalog = new CubeShapeCatalog(2, 2, Duration.ofSeconds(5), clock);
        var shape = new QueryShape(Set.of(), Set.of(), Set.of("SUM(bytes)"));
        var window = new TimeRange(0, DAY - 1);
        catalog.register(window, shape, total(100));
        now.set(Instant.ofEpochSecond(4));
        assertThat(catalog.tryAnswer(window, shape)).isPresent();
        now.set(Instant.ofEpochSecond(5));
        assertThat(catalog.tryAnswer(window, shape)).isEmpty();
    }

    @Test void validationDistinguishesSqlNullFromTheLiteralStringNull() {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        AtomicReference<String> value = new AtomicReference<>(null);
        var detector = new DataChangeDetector(backend, sql -> sql,
                sql -> ResultFrame.builder().column("v", ColumnType.STRING).row((Object) value.get()).build(),
                "validation {start} {end}", Duration.ofDays(1));
        var range = new TimeRange(0, DAY - 1);
        assertThat(detector.hasHistoricalDataChanged(range)).isTrue();
        value.set("null");
        assertThat(detector.hasHistoricalDataChanged(range)).isTrue();
    }

    @Test void cubeAnswerNeverOutlivesTheBucketItWasDerivedFrom() {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.ofEpochSecond(10 * DAY));
        Clock clock = new Clock() {
            public ZoneId getZone() { return ZoneOffset.UTC; }
            public Clock withZone(ZoneId zone) { return this; }
            public Instant instant() { return now.get(); }
        };
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer(), clock);
        var catalog = new CubeShapeCatalog(2, 2, Duration.ofMinutes(5), clock);
        var canonical = new CanonicalQueryObject(HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()),
                new TimeRange(0, DAY - 1), PostProcessing.none(), QueryMetadata.globalAggregate(), "full",
                List.of("traffic"), List.of());
        var hash = new QueryHashGenerator().generateQueryHash(canonical, 300);
        backend.store(CacheKeyFactory.buildBucketKey(hash, 0, DAY), total(100), Duration.ofSeconds(2));
        AtomicInteger sparkCalls = new AtomicInteger();
        var engine = new CacheExecutionEngine(backend, sql -> { sparkCalls.incrementAndGet(); return total(200); },
                (sql, gap) -> sql, CacheExecutionConfiguration.defaults(), null, catalog, Runnable::run, clock);
        assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(100);
        assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(100);
        assertThat(sparkCalls).hasValue(0);
        now.set(now.get().plusSeconds(3));
        assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(200);
        assertThat(sparkCalls).hasValue(1);
    }

    @Test void aConcurrentBucketMutationPreventsRegistrationOfAnObsoleteCube() {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var catalog = new CubeShapeCatalog();
        var canonical = query(QueryMetadata.globalAggregate());
        var hash = new QueryHashGenerator().generateQueryHash(canonical, 300);
        AtomicInteger calls = new AtomicInteger();
        var engine = new CacheExecutionEngine(backend, sql -> {
            int call = calls.incrementAndGet();
            if (call == 1) backend.store("QC:V4:B86400:unrelated:0", total(1), Duration.ofDays(1));
            return total(call == 1 ? 100 : 200);
        }, (sql, gap) -> sql, CacheExecutionConfiguration.defaults(), null, catalog);
        assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(100);
        assertThat(catalog.windowCount()).isZero();
        assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(200);
        assertThat(calls).hasValue(2);
    }

    @Test void aColdAverageUsesOneOriginalSparkQueryWhileTheWarmerOwnsIngredients() {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var metadata = QueryMetadata.globalAggregate().withMeasureAggregates(Map.of("mean", AggregateFunction.AVERAGE));
        var canonical = new CanonicalQueryObject(HashComponents.of(List.of(), List.of("SUM(x)", "COUNT(x)"), List.of()),
                new TimeRange(0, DAY - 1), PostProcessing.none(), metadata, "SELECT AVG(x) AS mean", List.of(), List.of());
        AtomicInteger calls = new AtomicInteger();
        var engine = new CacheExecutionEngine(backend, sql -> {
            assertThat(sql).isEqualTo("SELECT AVG(x) AS mean");
            calls.incrementAndGet();
            return ResultFrame.builder().column("mean", ColumnType.DOUBLE).row(1.25).build();
        }, (sql, gap) -> { throw new AssertionError("zero-hit AVG does not need gap rewriting"); }, CacheExecutionConfiguration.defaults());
        assertThat(engine.execute(canonical, HASH).doubleAt(0, 0)).isEqualTo(1.25);
        assertThat(calls).hasValue(1);
    }

    @Test void decimalCompositesStayAuthoritativeInsteadOfCachingUnprovenScaleArithmetic() {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var canonical = query(QueryMetadata.globalAggregate().withCompositeAliases(Map.of("total", "SUM(x)+SUM(y)")));
        var warmer = new WarmingOrchestrator(backend,
                sql -> ResultFrame.builder().column("SUM(x)", ColumnType.DECIMAL).column("SUM(y)", ColumnType.DECIMAL)
                        .row(new java.math.BigDecimal("1.123456789012345678"), new java.math.BigDecimal("2.0")).build(),
                (sql, gap) -> sql, new WarmingQueue(), new QueryPopularityTracker(), DAY, 10);
        assertThat(warmer.warmSinglePattern(HASH, canonical, 0, DAY - 1, false).bucketsSkipped()).isOne();
        assertThat(backend.storedBucketCount()).isZero();
    }
}
