package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.ValkeyQueryPopularityAdapter;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.service.CacheExecutionEngine;
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.query.*;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import static org.assertj.core.api.Assertions.*;

/** Opt-in real-server tests; use a dedicated disposable Valkey instance. */
@EnabledIfSystemProperty(named="cascada.valkey.url", matches=".+")
class ValkeyFreshnessIntegrationTest {
    private static final String URL = System.getProperty("cascada.valkey.url");
    private static ResultFrame value(long n) {
        return ResultFrame.builder().column("SUM(bytes)", ColumnType.LONG).row(n).build();
    }

    @Test void independentAdaptersShareAtomicRevisionsAndInvalidateTheCube() throws Exception {
        try (var reader = new ValkeyCacheBackendAdapter(URL, new PortableFrameSerializer());
             var writer = new ValkeyCacheBackendAdapter(URL, new PortableFrameSerializer())) {
            var canonical = new CanonicalQueryObject(HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()),
                    new TimeRange(0, 86399), PostProcessing.none(), QueryMetadata.globalAggregate(), "full",
                    List.of(UUID.randomUUID().toString()), List.of());
            var hash = new QueryHashGenerator().generateQueryHash(canonical, 300);
            String key = CacheKeyFactory.buildBucketKey(hash, 0, 86400);
            String before = reader.revision().orElseThrow();
            writer.store(key, value(100), Duration.ofSeconds(2));
            assertThat(reader.revision()).isEqualTo(writer.revision()).isNotEqualTo(Optional.of(before));
            assertThat(reader.remainingTimeToLive(List.of(key))).isPresent();
            var engine = new CacheExecutionEngine(reader, sql -> value(300), (sql, gap) -> sql,
                    CacheExecutionConfiguration.defaults(), null, new CubeShapeCatalog());
            assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(100);
            writer.store(key, value(200), Duration.ofSeconds(1));
            assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(200);
            Thread.sleep(1200);
            assertThat(reader.existsForKeys(List.of(key))).containsExactly(false);
            assertThat(engine.execute(canonical, hash).longAt(0, 0)).isEqualTo(300);
        }
    }

    @Test void popularityAccumulatesLocallyThenFlushesCountsInOneBatch() {
        io.lettuce.core.RedisClient client = io.lettuce.core.RedisClient.create(URL);
        try (var connection = client.connect(); var popularity = new ValkeyQueryPopularityAdapter(URL, Duration.ofDays(1))) {
            QueryHash hash = new QueryHashGenerator().generateQueryHash(new CanonicalQueryObject(
                    HashComponents.of(List.of(), List.of("SUM(x)"), List.of()), new TimeRange(0, 86399),
                    PostProcessing.none(), QueryMetadata.globalAggregate(), "sql", List.of(UUID.randomUUID().toString()), List.of()));
            for (int index = 0; index < 100; index++) popularity.recordObservation(hash, "SELECT SUM(x)");
            assertThat(connection.sync().zscore("QT:V1:TOP", hash.value())).isNull();
            assertThat(popularity.hitCount(hash)).isEqualTo(100);
            assertThat(connection.sync().zscore("QT:V1:TOP", hash.value())).isEqualTo(100.0);
            assertThat(popularity.topByPopularity(10)).anyMatch(query -> query.queryHash().equals(hash));
        } finally { client.shutdown(); }
    }

    @Test void aWarmStartedBeforeInvalidationCannotRestoreItsOldSnapshot() {
        try (var oldWriter = new ValkeyCacheBackendAdapter(URL, new PortableFrameSerializer());
             var invalidator = new ValkeyCacheBackendAdapter(URL, new PortableFrameSerializer())) {
            String key = "QC:V4:B86400:" + UUID.randomUUID().toString().replace("-", "") + ":0";
            String oldGeneration = oldWriter.warmingGeneration().orElseThrow();
            invalidator.flush(com.cascada.cache.domain.admin.CacheScope.everything());
            assertThat(oldWriter.storeForGeneration(key, value(100), Duration.ofMinutes(1), oldGeneration)).isFalse();
            assertThat(invalidator.existsForKeys(List.of(key))).containsExactly(false);
            String current = oldWriter.warmingGeneration().orElseThrow();
            assertThat(oldWriter.storeForGeneration(key, value(200), Duration.ofMinutes(1), current)).isTrue();
            assertThat(invalidator.multiGet(List.of(key)).get(0).orElseThrow().longAt(0, 0)).isEqualTo(200);
        }
    }
}
