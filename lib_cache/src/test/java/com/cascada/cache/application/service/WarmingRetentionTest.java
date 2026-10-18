package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.query.PopularQuery;
import com.cascada.identity.domain.model.QueryHash;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class WarmingRetentionTest {
    private static QueryHash hash(int number) { return QueryHash.of(String.format("%032x", number)); }
    @Test void concurrentObservationsAllowConsistentHashSnapshots() throws Exception {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var warmer = new WarmingOrchestrator(backend, sql -> ResultFrame.empty(), (sql, gap) -> sql,
                new WarmingQueue(256), new QueryPopularityTracker(256), null, 86_400, 3, 256);
        CanonicalQueryObject query = new CanonicalQueryObject(
                HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()), new TimeRange(0, 86399),
                PostProcessing.none(), QueryMetadata.globalAggregate(), "SELECT 1", List.of("traffic"), List.of());
        List<QueryHash> hashes = java.util.stream.IntStream.range(0, 256).mapToObj(WarmingRetentionTest::hash).toList();
        var start = new java.util.concurrent.CountDownLatch(1);
        try (var workers = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var writer = workers.submit(() -> {
                start.await();
                for (QueryHash queryHash : hashes) warmer.recordQuery(queryHash, query);
                return null;
            });
            var reader = workers.submit(() -> {
                start.await();
                for (int snapshot = 0; snapshot < 4096; snapshot++) {
                    assertThat(warmer.knownQueryHashes().size()).isLessThanOrEqualTo(256);
                }
                return null;
            });
            start.countDown();
            writer.get();
            reader.get();
        }
        assertThat(warmer.knownQueryHashes()).containsExactlyElementsOf(hashes);
    }
    @Test void boundsVotesPopularityAndCanonicalHistory() {
        WarmingQueue queue = new WarmingQueue(2);
        QueryPopularityTracker popularity = new QueryPopularityTracker(2);
        for (int number = 1; number <= 3; number++) { queue.vote(hash(number)); popularity.recordObservation(hash(number), "SELECT " + number); }
        assertThat(queue.consumeAll()).containsExactly(hash(2), hash(3));
        assertThat(popularity.distinctQueryCount()).isEqualTo(2);
        assertThat(popularity.hitCount(hash(1))).isZero();
        QueryPopularityPort stalePopularity = new QueryPopularityPort() {
            public void recordObservation(QueryHash query, String physicalSql) { }
            public long hitCount(QueryHash query) { return 0; }
            public List<PopularQuery> topByPopularity(int limit) {
                return List.of(new PopularQuery(hash(1), "SELECT 1"), new PopularQuery(hash(2), "SELECT 2"),
                        new PopularQuery(hash(3), "SELECT 3"));
            }
        };
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var warmer = new WarmingOrchestrator(backend,
                sql -> ResultFrame.builder().column("n", ColumnType.LONG).row(1L).build(),
                (sql, gap) -> sql, new WarmingQueue(2), stalePopularity, null, 86_400, 3, 2);
        for (int number = 1; number <= 3; number++) warmer.recordQuery(hash(number), new CanonicalQueryObject(
                HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()), new TimeRange(0, 86399),
                PostProcessing.none(), QueryMetadata.globalAggregate(), "SELECT " + number, List.of("traffic"), List.of()));
        var report = warmer.warmCycle(0, 86399, false);
        assertThat(report.patternsWarmed()).isEqualTo(2);
        assertThat(backend.sizeReport().bucketCount()).isEqualTo(2);
    }
}
