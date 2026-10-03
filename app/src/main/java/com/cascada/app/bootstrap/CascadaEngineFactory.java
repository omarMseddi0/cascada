package com.cascada.app.bootstrap;

import com.cascada.app.config.EngineSettings;
import com.cascada.app.config.CacheBackend;

import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.port.out.CacheBackendPort;
import com.cascada.cache.application.port.out.CoverageIndexPort;
import com.cascada.cache.application.port.out.GapQueryRewriterPort;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.cache.application.port.out.SqlCanonicalizerPort;
import com.cascada.cache.application.service.CacheAdministrationService;
import com.cascada.cache.application.service.CacheExecutionEngine;
import com.cascada.cache.application.service.ExecuteCachedQueryService;
import com.cascada.cache.application.service.ExecuteLogicalQueryService;
import com.cascada.cache.application.service.WarmingOrchestrator;
import com.cascada.cache.domain.cube.CubeShapeCatalog;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.SafetyRuleRegistry;
import com.cascada.cache.domain.warming.WarmingQueue;
import com.cascada.sql.adapter.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.calcite.GapQueryRewriterAdapter;
import com.cascada.sql.adapter.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.domain.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.domain.TimeDimensionMap;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

/** Composition root for cache use cases and their infrastructure adapters. */
public final class CascadaEngineFactory implements AutoCloseable {

    private final EngineSettings settings;

    // Outbound adapters, created once and shared by every service below.
    private final QueryExecutorPort queryExecutor;
    private final CacheBackendPort cacheBackend;
    private final CoverageIndexPort coverageIndex;
    private final QueryPopularityPort popularityTracker;
    private final TableCatalog tableCatalog;
    private final CubeShapeCatalog cubeCatalog;
    private final WarmingOrchestrator warmingOrchestrator;
    private final ExecuteCachedQueryUseCase cachedQueryUseCase;
    private final ExecuteLogicalQueryUseCase logicalQueryUseCase;
    private final CacheAdministrationService administrationService;
    private final ThreadPoolExecutor cacheTasks;

    /**
     * @param queryExecutor the execution tier. Passed in rather than built here because it is the one
     *     adapter whose construction needs a runtime that may be absent (a Spark session); a test injects
     *     a fake, {@code CascadaLauncher} injects the real Spark/Delta adapter.
     */
    public CascadaEngineFactory(EngineSettings settings, QueryExecutorPort queryExecutor) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.queryExecutor = Objects.requireNonNull(queryExecutor, "queryExecutor");
        // Validate configuration before opening the backend's network resources.
        CacheConfiguration validatedConfiguration = cacheConfiguration();
        this.tableCatalog = tableCatalog();
        this.cacheBackend = cacheBackend();
        this.coverageIndex = coverageIndex();
        this.popularityTracker = popularityTracker();
        this.cubeCatalog = new CubeShapeCatalog();
        this.cacheTasks = new ThreadPoolExecutor(2, 2, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(128), (task, executor) -> {
                    if (executor.isShutdown()) throw new RejectedExecutionException("cache executor is closed");
                    task.run();
                });
        this.warmingOrchestrator = new WarmingOrchestrator(cacheBackend, queryExecutor, gapQueryRewriter(),
                new WarmingQueue(), popularityTracker, coverageIndex, settings.cacheExecution().bucketSeconds(),
                settings.warmingTopNQueries());
        CacheExecutionEngine executionEngine = new CacheExecutionEngine(cacheBackend, queryExecutor,
                gapQueryRewriter(), settings.cacheExecution(), coverageIndex, cubeCatalog, cacheTasks);
        this.cachedQueryUseCase = new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(),
                validatedConfiguration, new QueryHashGenerator(), executionEngine, queryExecutor, warmingOrchestrator);
        this.logicalQueryUseCase = new ExecuteLogicalQueryService(translator(), canonicalizer(),
                cachedQueryUseCase, queryExecutor);
        this.administrationService = new CacheAdministrationService(cacheBackend, coverageIndex, cubeCatalog);
    }

    // ---------------------------------------------------------------------------------------------
    // 1. Outbound adapters — the only place a concrete adapter class is named.
    // ---------------------------------------------------------------------------------------------

    /**
     * The hot cache tier. Local runs use the in-memory blob backend, which stores the <em>same
     * serialized bytes</em> Valkey would — that fidelity is why a local run exercises the real
     * serialization path and not a shortcut.
     */
    private CacheBackendPort cacheBackend() {
        ArrowResultFrameSerializer serializer = new ArrowResultFrameSerializer();
        return settings.cacheBackend() == CacheBackend.MEMORY
                ? new InMemoryBlobCacheBackendAdapter(serializer)
                : new ValkeyCacheBackendAdapter(settings.redisUri(), serializer);
    }

    /**
     * The coverage-bitmap index: answers "which buckets of this query are cached?" in one fetch instead
     * of one existence check per bucket.
     *
     * <p>TODO(cascada): add a Valkey-backed CoverageIndexPort storing BucketCoverageBitmap.toBytes()
     * under CV:B&lt;seconds&gt;:&lt;hash&gt; and mutating it with SETBIT. Until then a cluster deployment
     * gets an index that is not shared between driver replicas, so each replica warms its own bitmap.
     * That costs redundant existence checks but never correctness, because the index is advisory.
     */
    private CoverageIndexPort coverageIndex() {
        return new InMemoryCoverageIndexAdapter();
    }

    /**
     * TODO(cascada): back this with a Redis sorted set (QT:V1:TOP) so popularity survives a restart.
     * In-memory means the Layer-2 warmer starts cold on every deploy and re-learns which queries matter.
     */
    private QueryPopularityPort popularityTracker() {
        return new QueryPopularityTracker();
    }

    /**
     * The logical→physical table registry.
     *
     * <p>TODO(cascada): load every table from deployment configuration.
     * Registering one table from settings is enough to run, but it means a customer can only query a
     * single table, and the column map below is a placeholder identity mapping rather than the real
     * logical→physical schema.
     */
    private TableCatalog tableCatalog() {
        return new TableCatalog().register(RegisteredTable.of(
                settings.mainTableName(),
                settings.mainTablePath(),
                Map.of(settings.cacheExecution().timeColumnName(), settings.cacheExecution().timeColumnName()),
                settings.cacheExecution().timeColumnName()));
    }

    /** Calcite canonicalisation, behind the cache's port. */
    private SqlCanonicalizerPort canonicalizer() {
        return new CalciteCanonicalObjectFactory(new TimeDimensionMap(
                java.util.Set.of(settings.cacheExecution().timeColumnName())));
    }

    /**
     * Logical→physical translation, behind the cache's port.
     *
     * <p>The bucket step handed to the translator is the <b>fixed internal step</b>, not the bucket
     * width. The translator emits {@code FLOOR(ts/step)*step} rows, the cache stores them at that same
     * step, and the merge resamples up to whatever the user asked for. Passing the bucket width here
     * instead would make the SQL produce day-granular rows while the merge expected step-granular ones.
     */
    private LogicalSqlTranslatorPort translator() {
        return new LogicalToPhysicalSqlTranslator(settings.cacheExecution().fixedStepSeconds(), tableCatalog);
    }

    /** Gap-query rewriting, behind the cache's port. */
    private GapQueryRewriterPort gapQueryRewriter() {
        return new GapQueryRewriterAdapter(
                settings.cacheExecution().timeColumnName(), settings.cacheExecution().bucketSeconds());
    }

    // ---------------------------------------------------------------------------------------------
    // 2 + 3. Application services, returned as inbound ports.
    // ---------------------------------------------------------------------------------------------

    /** The read path for an already-canonicalised query. */
    public ExecuteCachedQueryUseCase executeCachedQueryUseCase() {
        return cachedQueryUseCase;
    }

    /** The read path for logical SQL — what a REST or JDBC adapter should drive. */
    public ExecuteLogicalQueryUseCase executeLogicalQueryUseCase() {
        return logicalQueryUseCase;
    }

    /** The administrator console's size measurement. */
    public MeasureCacheSizeUseCase measureCacheSizeUseCase() {
        return administrationService;
    }

    /** The administrator console's flush action. */
    public FlushCacheUseCase flushCacheUseCase() {
        return administrationService;
    }

    /** Shared warmer records queries during this factory's lifetime. Scheduling is owned by callers. */
    public WarmCacheUseCase warmCacheUseCase() {
        return warmingOrchestrator;
    }

    /** Exposed so a launcher can close adapters that hold sockets or sessions. */
    public CacheBackendPort cacheBackendForShutdown() {
        return cacheBackend;
    }

    private CacheConfiguration cacheConfiguration() {
        long bucketSeconds = settings.cacheExecution().bucketSeconds();
        if (bucketSeconds % 3_600L != 0) {
            throw new IllegalArgumentException("CASCADA_BUCKET_SECONDS must be a whole number of hours");
        }
        CacheConfiguration defaults = CacheConfiguration.defaults();
        return new CacheConfiguration(defaults.impossibleAggregates(), defaults.highCardinalityColumns(),
                defaults.liquidClusteredFilterColumns(), settings.cacheExecution().fixedStepSeconds(),
                Math.toIntExact(bucketSeconds / 3_600L), defaults.minimumCacheableTimeRangeSeconds());
    }

    @Override
    public void close() {
        RuntimeException shutdownFailure = null;
        try {
            closeCacheTasks();
        } catch (RuntimeException failure) {
            shutdownFailure = failure;
        }
        if (cacheBackend instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception failure) {
                if (shutdownFailure == null) {
                    shutdownFailure = new IllegalStateException("failed to close cache backend", failure);
                } else {
                    shutdownFailure.addSuppressed(failure);
                }
            }
        }
        if (shutdownFailure != null) throw shutdownFailure;
    }

    private void closeCacheTasks() {
        cacheTasks.shutdown();
        try {
            if (!cacheTasks.awaitTermination(30, TimeUnit.SECONDS)) {
                cacheTasks.shutdownNow();
                throw new IllegalStateException("cache tasks did not finish before shutdown");
            }
        } catch (InterruptedException interrupted) {
            cacheTasks.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing cache tasks", interrupted);
        }
    }
}
