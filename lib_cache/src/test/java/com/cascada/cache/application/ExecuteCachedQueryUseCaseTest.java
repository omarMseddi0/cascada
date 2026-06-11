package com.cascada.cache.application;

import com.cascada.cache.adapter.backend.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.SafetyRule;
import com.cascada.cache.domain.safety.SafetyRuleRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The application entry point's two branches: a safety bypass runs the physical SQL directly and says
 * so ({@code servedThroughCache=false}); a safe query goes through the cache engine.
 */
class ExecuteCachedQueryUseCaseTest {

    private static final long DAY = 86_400L;

    private final PortableFrameSerializer serializer = new PortableFrameSerializer();

    private CanonicalQueryObject canonical() {
        HashComponents components = HashComponents.of(List.of("appName"), List.of("SUM(bytes)"), List.of());
        return new CanonicalQueryObject(components, new TimeRange(0, 3 * DAY - 1), PostProcessing.none(),
                QueryMetadata.globalAggregate(), "FULL_SQL", List.of("traffic"), List.of());
    }

    private ResultFrame frame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("bytes", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "bytes", 42.0))
                .build();
    }

    private ExecuteCachedQueryUseCase useCase(SafetyRuleRegistry registry, SparkQueryExecutorPort spark) {
        CacheExecutionEngine engine = new CacheExecutionEngine(
                new InMemoryBlobCacheBackendAdapter(serializer), spark,
                (physicalSql, gapPlan) -> "GAP_SQL", CacheExecutionConfiguration.defaults());
        return new ExecuteCachedQueryUseCase(registry, CacheConfiguration.defaults(),
                new QueryHashGenerator(), engine, spark);
    }

    @Test
    void aSafetyBypassRunsThePhysicalSqlDirectlyAndReportsNotCached() {
        SafetyRule bypassEverything = new SafetyRule() {
            @Override
            public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject,
                                                   CacheConfiguration configuration) {
                return Optional.of(BypassReason.IMPOSSIBLE_MATH);
            }

            @Override
            public String ruleName() {
                return "test-always-bypass";
            }
        };
        SafetyRuleRegistry alwaysBypass = new SafetyRuleRegistry(List.of(bypassEverything));
        StringBuilder seenSql = new StringBuilder();
        SparkQueryExecutorPort spark = sql -> {
            seenSql.append(sql);
            return frame();
        };

        ExecuteCachedQueryUseCase.Result result = useCase(alwaysBypass, spark).execute(canonical());

        assertThat(result.servedThroughCache()).isFalse();
        assertThat(seenSql.toString()).isEqualTo("FULL_SQL");
        assertThat(result.frame().rowCount()).isEqualTo(1);
    }

    @Test
    void aSafeQueryGoesThroughTheCacheEngineAndReportsCached() {
        SafetyRuleRegistry neverBypass = new SafetyRuleRegistry(List.of());
        AtomicInteger sparkCalls = new AtomicInteger();
        SparkQueryExecutorPort spark = sql -> {
            sparkCalls.incrementAndGet();
            return frame();
        };

        ExecuteCachedQueryUseCase.Result result = useCase(neverBypass, spark).execute(canonical());

        assertThat(result.servedThroughCache()).isTrue();
        assertThat(sparkCalls.get()).isEqualTo(1); // zero cached buckets -> engine's direct fast path
        assertThat(((Number) result.frame().rows().get(0).get("bytes")).doubleValue()).isEqualTo(42.0);
    }
}
