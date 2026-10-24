package com.cascada.app.config;

import com.cascada.app.model.config.EngineSettings;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.model.QueryResult;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @AfterEach
    void closeEngine() {
        factory.close();
    }

    @BeforeEach
    void wireEngine() {
        executor = new CapturingExecutor();
        // Literal settings, no environment read: the factory is deterministic by construction.
        EngineSettings settings = new EngineSettings(
                com.cascada.cache.application.model.config.CacheExecutionConfiguration.defaults(),
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
        QueryResult result = factory.executeLogicalQueryUseCase().query(LOGICAL_SQL);

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
    void theFactoryHandsBackPortsRatherThanServiceClasses() throws Exception {
        assertThat(CascadaEngineFactory.class.getMethod("executeLogicalQueryUseCase").getReturnType())
                .isEqualTo(ExecuteLogicalQueryUseCase.class);
    }

    @Test
    void anEmptyCacheStillAnswersCorrectlyByBypassingToTheExecutor() {
        // Cold start is the case most likely to be broken by a wiring mistake: no buckets exist, so the
        // engine must fall through to the executor rather than returning an empty frame.
        QueryResult result = factory.executeLogicalQueryUseCase().query(LOGICAL_SQL);
        assertThat(result.frame().rowCount()).isEqualTo(2);
    }

    @Test
    void configuredTimeColumnIsRecognisedByCanonicalization() {
        EngineSettings settings = new EngineSettings(
                new com.cascada.cache.application.model.config.CacheExecutionConfiguration(86_400, 300, "event_time"),
                "redis://unused:6379", "traffic", "/tmp/traffic", true, 10);
        try (CascadaEngineFactory configured = new CascadaEngineFactory(settings, executor)) {
            QueryResult result = configured.executeLogicalQueryUseCase().query(
                    "SELECT SUM(bytes) AS total_bytes FROM traffic WHERE event_time >= 0 AND event_time <= 86399");
            assertThat(result.servedThroughCache()).isTrue();
        }
    }    @Test
    void ownsASeparateCpuExecutorAndTerminatesItOnClose() throws Exception {
        var cpuField = CascadaEngineFactory.class.getDeclaredField("mergeTasks");
        cpuField.setAccessible(true);
        var cpu = (java.util.concurrent.ThreadPoolExecutor) cpuField.get(factory);
        var ioField = CascadaEngineFactory.class.getDeclaredField("cacheTasks");
        ioField.setAccessible(true);
        var io = (java.util.concurrent.ExecutorService) ioField.get(factory);
        var decodeField = CascadaEngineFactory.class.getDeclaredField("decodeTasks");
        decodeField.setAccessible(true);
        var decode = (java.util.concurrent.ThreadPoolExecutor) decodeField.get(factory);
        assertThat(decode).isNotSameAs(cpu);
        assertThat(decode.getCorePoolSize()).isEqualTo(Math.max(1, Math.min(4, cpu.getCorePoolSize())));
        assertThat(decode.submit(() -> Thread.currentThread().isVirtual()).get()).isFalse();
        assertThat(cpu.submit(() -> Thread.currentThread().isVirtual()).get()).isFalse();
        assertThat(cpu.submit(() -> Thread.currentThread().getName()).get()).startsWith("cache-merge-");
        assertThat(io.submit(() -> Thread.currentThread().isVirtual()).get()).isTrue();
        assertThat(cpu.getMaximumPoolSize()).isLessThanOrEqualTo(Runtime.getRuntime().availableProcessors());
        factory.close();
        assertThat(cpu.isTerminated()).isTrue();
        assertThat(decode.isTerminated()).isTrue();
        assertThat(io.isTerminated()).isTrue();
    }


    @Test
    void sharesAnActiveQueryBudgetWithTheExecutionEngine() throws Exception {
        var budgetField = CascadaEngineFactory.class.getDeclaredField("queryAdmissionBudget");
        budgetField.setAccessible(true);
        var budget = (com.cascada.cache.application.execution.QueryAdmissionBudget) budgetField.get(factory);
        var cpuField = CascadaEngineFactory.class.getDeclaredField("mergeTasks");
        cpuField.setAccessible(true);
        var cpu = (java.util.concurrent.ThreadPoolExecutor) cpuField.get(factory);
        assertThat(budget.maximumConcurrentQueries()).isEqualTo(Math.max(1, Math.min(4, cpu.getCorePoolSize())));
        var engineField = com.cascada.cache.application.service.ExecuteCachedQueryService.class
                .getDeclaredField("cacheExecutionEngine");
        engineField.setAccessible(true);
        Object engine = engineField.get(factory.executeCachedQueryUseCase());
        var engineBudget = com.cascada.cache.application.service.CacheExecutionEngine.class
                .getDeclaredField("queryAdmissionBudget");
        engineBudget.setAccessible(true);
        assertThat(engineBudget.get(engine)).isSameAs(budget);
    }

    @Test
    void explicitAdmissionLimitIsIndependentOfTheMergeWorkerCount() throws Exception {
        try (CascadaEngineFactory configured = new CascadaEngineFactory(EngineSettings.localDefaults(), executor,
                com.cascada.app.model.config.ApplicationSettings.defaults(), 2)) {
            var field = CascadaEngineFactory.class.getDeclaredField("queryAdmissionBudget");
            field.setAccessible(true);
            assertThat(((com.cascada.cache.application.execution.QueryAdmissionBudget) field.get(configured))
                    .maximumConcurrentQueries()).isEqualTo(2);
        }
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new CascadaEngineFactory(EngineSettings.localDefaults(), executor,
                        com.cascada.app.model.config.ApplicationSettings.defaults(), 0));
    }

    @Test
    void remoteCacheAdmissionDefaultAccountsForItsSeparateDecodeStage() throws Exception {
        EngineSettings remote = new EngineSettings(
                new com.cascada.cache.application.model.config.CacheExecutionConfiguration(86400, 300, "ts", 4),
                "redis://unused:6379", "traffic", "/tmp/traffic", false, 10);
        var defaults = CascadaEngineFactory.class.getDeclaredMethod("defaultMaximumConcurrentQueries", EngineSettings.class);
        defaults.setAccessible(true);
        int workers = Math.min(4, Runtime.getRuntime().availableProcessors());
        assertThat((Integer) defaults.invoke(null, remote)).isEqualTo(Math.min(8, workers * 2));
    }
}
