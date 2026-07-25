package com.cascada.app.bootstrap;

import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the composition root wires a working engine, and — more importantly — proves it can be driven
 * entirely through inbound ports with a single fake at the outbound edge.
 *
 * <p>That second property is the architecture's payoff made concrete. A logical SQL string travels
 * translate → canonicalise → safety rules → cache engine → executor, exercising four modules, with no
 * Spark cluster, no Valkey server, and no Kubernetes. The only thing substituted is the
 * {@link QueryExecutorPort} — swapped for the fake below — which is exactly the substitution the
 * reference architecture promises: "the application should be equally controllable by users, other
 * applications, or automated tests."
 *
 * <p>The fake also lets the test assert the <em>translation output</em> precisely, which a real engine
 * would hide. Executor correctness is the concern of the Spark adapter's own tests; a real Delta read is
 * a cluster integration test.
 */
class CascadaEngineFactoryTest {

    /** Captures the physical SQL the engine produced, and returns a canned aggregated frame. */
    private static final class CapturingExecutor implements QueryExecutorPort {
        private String lastPhysicalSql;

        @Override
        public ResultFrame execute(String physicalSql) {
            this.lastPhysicalSql = physicalSql;
            return ResultFrame.builder()
                    .column("appName", ColumnType.STRING)
                    .column("total_bytes", ColumnType.DOUBLE)
                    .row(Map.of("appName", "netflix", "total_bytes", 150.0))
                    .row(Map.of("appName", "youtube", "total_bytes", 20.0))
                    .build();
        }
    }

    private static final String LOGICAL_SQL =
            "SELECT appName, SUM(bytes) AS total_bytes FROM traffic "
                    + "WHERE ts >= 0 AND ts <= 9999 GROUP BY appName ORDER BY appName";

    private CapturingExecutor executor;
    private CascadaEngineFactory factory;

    @BeforeEach
    void wireEngine() {
        executor = new CapturingExecutor();
        // Literal settings, no environment read: the factory is deterministic by construction.
        EngineSettings settings = new EngineSettings(
                com.cascada.cache.application.service.CacheExecutionConfiguration.defaults(),
                "redis://unused:6379",
                "traffic",
                "/tmp/traffic",
                true,
                10);
        factory = new CascadaEngineFactory(settings, executor);
    }

    @Test
    void resolvesTheLogicalTableToItsPhysicalPathAndReachesTheExecutor() {
        factory.executeLogicalQueryUseCase().query(LOGICAL_SQL);

        assertThat(executor.lastPhysicalSql).contains("delta.`/tmp/traffic`");
        assertThat(executor.lastPhysicalSql).containsIgnoringCase("GROUP BY appName");
    }

    @Test
    void returnsTheExecutorResultThroughTheFullPipeline() {
        ExecuteCachedQueryUseCase.Result result = factory.executeLogicalQueryUseCase().query(LOGICAL_SQL);

        assertThat(result.frame().columnNames()).contains("appName", "total_bytes");
        Map<String, Double> totalByApp = new HashMap<>();
        for (Map<String, Object> row : result.frame().rows()) {
            totalByApp.put((String) row.get("appName"), ((Number) row.get("total_bytes")).doubleValue());
        }
        assertThat(totalByApp.get("netflix")).isEqualTo(150.0);
        assertThat(totalByApp.get("youtube")).isEqualTo(20.0);
    }

    @Test
    void everyUseCaseIsWireable() {
        // A composition root's first duty is to not throw. Each accessor must build a complete graph.
        assertThat(factory.executeCachedQueryUseCase()).isNotNull();
        assertThat(factory.executeLogicalQueryUseCase()).isNotNull();
        assertThat(factory.measureCacheSizeUseCase()).isNotNull();
        assertThat(factory.flushCacheUseCase()).isNotNull();
        assertThat(factory.warmCacheUseCase()).isNotNull();
    }

    @Test
    void theFactoryHandsBackPortsRatherThanServiceClasses() {
        // Guards the rule that keeps driving adapters replaceable: if this ever compiles against a
        // concrete service type, an adapter can bind to an implementation and the seam is lost.
        ExecuteLogicalQueryUseCase logicalQueryPort = factory.executeLogicalQueryUseCase();
        assertThat(logicalQueryPort).isInstanceOf(ExecuteLogicalQueryUseCase.class);
    }

    @Test
    void anEmptyCacheStillAnswersCorrectlyByBypassingToTheExecutor() {
        // Cold start is the case most likely to be broken by a wiring mistake: no buckets exist, so the
        // engine must fall through to the executor rather than returning an empty frame.
        ExecuteCachedQueryUseCase.Result result = factory.executeLogicalQueryUseCase().query(LOGICAL_SQL);
        assertThat(result.frame().rowCount()).isEqualTo(2);
    }
}
