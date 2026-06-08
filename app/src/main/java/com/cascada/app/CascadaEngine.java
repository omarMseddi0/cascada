package com.cascada.app;

import com.cascada.cache.application.CacheExecutionConfiguration;
import com.cascada.cache.application.CacheExecutionEngine;
import com.cascada.cache.application.ExecuteCachedQueryUseCase;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.CacheBackendPort;
import com.cascada.cache.domain.port.GapQueryRewriterPort;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.SafetyRuleRegistry;
import com.cascada.sql.canonical.CalciteCanonicalObjectFactory;
import com.cascada.sql.rewrite.GapQueryRewriterAdapter;
import com.cascada.sql.translate.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.translate.TableCatalog;

/**
 * The composition root — the "main code that calls the library." It wires the four cascada libraries
 * into one end-to-end flow and exposes a single {@link #query(String)} method:
 *
 * <pre>
 *   logical SQL ──▶ logical→physical translate (lib_sql, delta.`/path` + bucketed time)
 *               ──▶ canonicalize (lib_sql → CanonicalQueryObject)
 *               ──▶ safety + cache engine (lib_cache: partial-hit = cached buckets ⊕ executor gap)
 *               ──▶ executor (the injected SparkQueryExecutorPort)
 * </pre>
 *
 * <p><b>Local and cluster are the same engine.</b> What differs is only which ports are injected here:
 * a {@code DuckDbInProcessQueryExecutor} or a local {@code SparkDeltaQueryExecutor} for dev; a
 * Kubernetes {@code SparkDeltaQueryExecutor} + a {@code ValkeyCacheBackendAdapter} for production. The
 * builder ({@link Builder}) is the one place that choice is made — see {@link CascadaApplication} for a
 * runnable wiring.
 */
public final class CascadaEngine {

    private final LogicalToPhysicalSqlTranslator translator;
    private final TableCatalog tableCatalog;
    private final CalciteCanonicalObjectFactory canonicalObjectFactory;
    private final ExecuteCachedQueryUseCase useCase;

    private CascadaEngine(LogicalToPhysicalSqlTranslator translator, TableCatalog tableCatalog,
                          CalciteCanonicalObjectFactory canonicalObjectFactory,
                          ExecuteCachedQueryUseCase useCase) {
        this.translator = translator;
        this.tableCatalog = tableCatalog;
        this.canonicalObjectFactory = canonicalObjectFactory;
        this.useCase = useCase;
    }

    /**
     * Run a logical SQL query end-to-end and return the result frame plus whether it was served through
     * the cache (false = a safety bypass straight to the executor).
     */
    public ExecuteCachedQueryUseCase.Result query(String logicalSql) {
        String physicalSql = translator.translate(logicalSql, tableCatalog);
        CanonicalQueryObject canonicalObject = canonicalObjectFactory.extractCanonicalObjectFromSql(physicalSql);
        return useCase.execute(canonicalObject);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Wires the engine from the ports you choose (the local-vs-cluster decision lives here). */
    public static final class Builder {
        private SparkQueryExecutorPort executor;
        private CacheBackendPort cacheBackend;
        private TableCatalog tableCatalog;
        private CacheExecutionConfiguration executionConfiguration = CacheExecutionConfiguration.defaults();
        private CacheConfiguration cacheConfiguration = CacheConfiguration.defaults();
        private SafetyRuleRegistry safetyRuleRegistry = SafetyRuleRegistry.defaultRegistry();

        /** The execution tier: a local/cluster Spark adapter, or the in-process DuckDB executor. */
        public Builder executor(SparkQueryExecutorPort executor) {
            this.executor = executor;
            return this;
        }

        /** The cache hot tier: the in-memory backend (dev) or the Valkey/Redis backend (production). */
        public Builder cacheBackend(CacheBackendPort cacheBackend) {
            this.cacheBackend = cacheBackend;
            return this;
        }

        /** Logical→physical table registry (logical name → Delta path + column map). */
        public Builder tableCatalog(TableCatalog tableCatalog) {
            this.tableCatalog = tableCatalog;
            return this;
        }

        public Builder executionConfiguration(CacheExecutionConfiguration executionConfiguration) {
            this.executionConfiguration = executionConfiguration;
            return this;
        }

        public Builder cacheConfiguration(CacheConfiguration cacheConfiguration) {
            this.cacheConfiguration = cacheConfiguration;
            return this;
        }

        public Builder safetyRuleRegistry(SafetyRuleRegistry safetyRuleRegistry) {
            this.safetyRuleRegistry = safetyRuleRegistry;
            return this;
        }

        public CascadaEngine build() {
            if (executor == null || cacheBackend == null || tableCatalog == null) {
                throw new IllegalStateException("executor, cacheBackend and tableCatalog are required");
            }
            GapQueryRewriterPort gapRewriter = new GapQueryRewriterAdapter(
                    executionConfiguration.timeColumnName(), executionConfiguration.bucketSeconds());
            CacheExecutionEngine cacheExecutionEngine =
                    new CacheExecutionEngine(cacheBackend, executor, gapRewriter, executionConfiguration);
            QueryHashGenerator queryHashGenerator = new QueryHashGenerator();
            ExecuteCachedQueryUseCase useCase = new ExecuteCachedQueryUseCase(
                    safetyRuleRegistry, cacheConfiguration, queryHashGenerator, cacheExecutionEngine, executor);
            LogicalToPhysicalSqlTranslator translator =
                    new LogicalToPhysicalSqlTranslator((int) executionConfiguration.bucketSeconds());
            return new CascadaEngine(translator, tableCatalog,
                    new CalciteCanonicalObjectFactory(), useCase);
        }
    }
}
