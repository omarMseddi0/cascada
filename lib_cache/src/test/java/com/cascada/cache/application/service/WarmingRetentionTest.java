package com.cascada.cache.application.service;

import com.cascada.cache.application.result.WarmingReport;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.identity.domain.QueryHash;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

class WarmingRetentionTest {
    private static QueryHash hash(int number) {
        return QueryHash.of(String.format("%032x", number));
    }

    @Test
    void concurrentObservationsAllowConsistentHashSnapshots() throws Exception {
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var warmer = new WarmingOrchestrator(backend, sql -> ResultFrame.empty(), (sql, gap) -> sql,
                new WarmingQueue(256), new QueryPopularityTracker(256), null, 86_400, 3, 256);
        CanonicalQueryObject query = new CanonicalQueryObject(
                HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()), new TimeRange(0, 86399),
                PostProcessing.none(), QueryMetadata.globalAggregate(), "SELECT 1", List.of("traffic"), List.of());
        List<QueryHash> hashes = java.util.stream.IntStream.range(0, 256)
                .mapToObj(WarmingRetentionTest::hash).toList();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            var writer = workers.submit(() -> {
                start.await();
                for (QueryHash queryHash : hashes) {
                    warmer.recordQuery(queryHash, query);
                }
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
        } finally {
            workers.shutdownNow();
        }
        assertThat(warmer.knownQueryHashes()).containsExactlyElementsOf(hashes);
    }

    @Test
    void boundsVotesPopularityAndCanonicalHistory() {
        WarmingQueue queue = new WarmingQueue(2);
        QueryPopularityTracker popularity = new QueryPopularityTracker(2);
        for (int number = 1; number <= 3; number++) {
            queue.vote(hash(number));
            popularity.recordObservation(hash(number));
        }
        assertThat(queue.consumeAll()).containsExactly(hash(2), hash(3));
        assertThat(popularity.distinctQueryCount()).isEqualTo(2);
        assertThat(popularity.hitCount(hash(1))).isZero();

        QueryPopularityPort stalePopularity = new QueryPopularityPort() {
            @Override
            public void recordObservation(QueryHash query) {
            }

            @Override
            public long hitCount(QueryHash query) {
                return 0;
            }

            @Override
            public List<QueryHash> topByPopularity(int limit) {
                return List.of(hash(1), hash(2), hash(3));
            }
        };
        var backend = new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer());
        var warmer = new WarmingOrchestrator(backend,
                sql -> ResultFrame.builder().column("n", ColumnType.LONG).row(1L).build(),
                (sql, gap) -> sql, new WarmingQueue(2), stalePopularity, null, 86_400, 3, 2);
        for (int number = 1; number <= 3; number++) {
            warmer.recordQuery(hash(number), new CanonicalQueryObject(
                    HashComponents.of(List.of(), List.of("SUM(bytes)"), List.of()), new TimeRange(0, 86399),
                    PostProcessing.none(), QueryMetadata.globalAggregate(), "SELECT " + number,
                    List.of("traffic"), List.of()));
        }

        WarmingReport report = warmer.warmCycle(0, 86399, false);
        assertThat(report.patternsWarmed()).isEqualTo(2);
        assertThat(backend.sizeReport().bucketCount()).isEqualTo(2);
    }
}