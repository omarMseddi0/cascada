package com.cascada.cache.application.service;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.model.QueryResult;
import com.cascada.cache.application.model.WarmingReport;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.query.PostProcessing;
import com.cascada.cache.domain.model.query.QueryMetadata;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.identity.domain.model.QueryHash;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** A query bypassed only for being short is still voted, so warming builds buckets a longer query will hit. */
class SmallRangeWarmingVoteTest {

    private static final long DAY = 86_400L;

    private final List<QueryHash> votes = new ArrayList<>();
    private final QueryExecutorPort spark = sql -> ResultFrame.builder()
            .column("appName", ColumnType.STRING).column("bytes", ColumnType.DOUBLE).row("netflix", 1.0).build();
    private final WarmCacheUseCase votingWarmer = new WarmCacheUseCase() {
        @Override public void recordQuery(QueryHash queryHash, CanonicalQueryObject canonicalObject) {
            votes.add(queryHash);
        }
        @Override public WarmingReport warmCycle(long start, long end, boolean forceOverwrite) {
            return new WarmingReport(0, 0, 0);
        }
        @Override public WarmingReport warmLookbackWindow(long nowSeconds, Duration lookback) {
            return new WarmingReport(0, 0, 0);
        }
    };

    private CanonicalQueryObject oneDayBy(String groupBy) {
        return new CanonicalQueryObject(HashComponents.of(List.of(groupBy), List.of("SUM(bytes)"), List.of()),
                new TimeRange(0, DAY - 1), PostProcessing.none(), QueryMetadata.globalAggregate(), "FULL_SQL",
                List.of("traffic"), List.of());
    }

    private QueryResult execute(CacheConfiguration configuration, CanonicalQueryObject query) {
        CacheExecutionEngine engine = new CacheExecutionEngine(
                new InMemoryBlobCacheBackendAdapter(new PortableFrameSerializer()), spark,
                (sql, gapPlan) -> "GAP_SQL", CacheExecutionConfiguration.defaults());
        return new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(), configuration,
                new QueryHashGenerator(), engine, spark, votingWarmer).execute(query);
    }

    @Test
    void aQueryShorterThanTheMinimumBypassesButIsVoted() {
        QueryResult result = execute(CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(2 * DAY),
                oneDayBy("appName"));

        assertThat(result.servedThroughCache()).isFalse();
        assertThat(votes).hasSize(1);
    }

    @Test
    void aShortQueryThatAlsoBreaksAHardRuleIsNotVoted() {
        execute(CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(2 * DAY)
                .withHighCardinalityColumns(Set.of("subscriberId")), oneDayBy("subscriberId"));

        assertThat(votes).isEmpty();
    }

    @Test
    void aMinimumOfZeroDisablesTheCacheAndTheVote() {
        execute(CacheConfiguration.defaults().withMinimumCacheableTimeRangeSeconds(0), oneDayBy("appName"));

        assertThat(votes).isEmpty();
    }
}
