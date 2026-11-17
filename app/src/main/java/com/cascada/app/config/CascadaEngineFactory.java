package com.cascada.app.config;

import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.model.config.CacheBackend;
import com.cascada.app.model.config.CacheSafetySettings;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.app.model.config.TableSettings;
import com.cascada.app.model.config.WarmingSettings;
import com.cascada.cache.adapter.out.cache.InMemoryBlobCacheBackendAdapter;
import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.index.InMemoryCoverageIndexAdapter;
import com.cascada.cache.adapter.out.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.adapter.out.tracking.QueryPopularityTracker;
import com.cascada.cache.adapter.out.tracking.ValkeyQueryPopularityAdapter;
import com.cascada.cache.application.model.config.WarmingConfiguration;
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
import com.cascada.cache.application.service.cube.CubeShapeCatalog;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.application.service.warming.DataChangeDetector;
import com.cascada.cache.application.service.warming.WarmingQueue;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.sql.adapter.out.calcite.GapQueryRewriterAdapter;
import com.cascada.sql.adapter.out.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.domain.model.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.domain.model.TimeDimensionMap;
import java.time.Clock;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Composition root for cache use cases and their infrastructure adapters. */
public final class CascadaEngineFactory implements AutoCloseable {

    private final EngineSettings settings;
    private final ApplicationSettings applicationSettings;

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
    private final ExecutorService cacheTasks;
    private final ThreadPoolExecutor mergeTasks;
    private final ThreadPoolExecutor decodeTasks;

    /**
     * @param queryExecutor the execution tier. Passed in rather than built here because it is the one
     *     adapter whose construction needs a runtime that may be absent (a Spark session); a test injects
     *     a fake, {@code CascadaLauncher} injects the real Spark/Delta adapter.
     */
    public CascadaEngineFactory(EngineSettings settings, QueryExecutorPort queryExecutor) {
        this(settings, queryExecutor, ApplicationSettings.defaults());
    }

    public CascadaEngineFactory(EngineSettings settings, QueryExecutorPort queryExecutor, ApplicationSettings applicationSettings) {
        this.applicationSettings = Objects.requireNonNull(applicationSettings);
        this.settings = Objects.requireNonNull(settings, "settings");
        this.queryExecutor = Objects.requireNonNull(queryExecutor, "queryExecutor");
        // Validate configuration before opening the backend's network resources.
        CacheConfiguration validatedConfiguration = resolveCacheSafetyConfiguration();
        this.tableCatalog = createTableCatalog();
        this.cacheTasks = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("cache-gap-", 0).factory());
        int mergeThreads = Math.min(settings.cacheExecution().mergeWorkerCount(),
                Runtime.getRuntime().availableProcessors());
        this.mergeTasks = createCpuPool(mergeThreads, "cache-merge-");
        int decodeThreads = Math.max(1, Math.min(4, mergeThreads));
        this.decodeTasks = createCpuPool(decodeThreads, "cache-decode-");
        CacheBackendPort openedBackend = null;
        try {
            openedBackend = createCacheBackend();
            this.cacheBackend = openedBackend;
            this.coverageIndex = new InMemoryCoverageIndexAdapter();
            this.popularityTracker = createPopularityTracker();
        } catch (RuntimeException | Error failure) {
            cacheTasks.shutdownNow();
            decodeTasks.shutdownNow();
            mergeTasks.shutdownNow();
            if (openedBackend instanceof AutoCloseable closeable) {
                try { closeable.close(); }
                catch (Exception cleanupFailure) { failure.addSuppressed(cleanupFailure); }
            }
            throw failure;
        }
        this.cubeCatalog = new CubeShapeCatalog();
        GapQueryRewriterPort gapQueryRewriter = createGapQueryRewriter();
        this.warmingOrchestrator = createWarmingOrchestrator(gapQueryRewriter, validatedConfiguration);
        CacheExecutionEngine executionEngine = new CacheExecutionEngine(cacheBackend, queryExecutor,
                gapQueryRewriter, settings.cacheExecution(), coverageIndex, cubeCatalog, cacheTasks,
                Clock.systemUTC(), mergeTasks);
        this.cachedQueryUseCase = new ExecuteCachedQueryService(SafetyRuleRegistry.defaultRegistry(),
                validatedConfiguration, new QueryHashGenerator(), executionEngine, queryExecutor,
                applicationSettings.warming().enabled() ? warmingOrchestrator : null);
        this.logicalQueryUseCase = new ExecuteLogicalQueryService(createTranslator(), createCanonicalizer(),
                cachedQueryUseCase, queryExecutor);
        this.administrationService = new CacheAdministrationService(cacheBackend, coverageIndex, cubeCatalog);
        mergeTasks.prestartAllCoreThreads();
        if (settings.cacheBackend() != CacheBackend.MEMORY) decodeTasks.prestartAllCoreThreads();
    }

    private static ThreadPoolExecutor createCpuPool(int workers, String threadNamePrefix) {
        return new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), Thread.ofPlatform().name(threadNamePrefix, 0).factory());
    }

    private WarmingOrchestrator createWarmingOrchestrator(GapQueryRewriterPort gapQueryRewriter,
                                                          CacheConfiguration safetyConfiguration) {
        WarmingSettings warming = applicationSettings.warming();
        return new WarmingOrchestrator(cacheBackend, queryExecutor, gapQueryRewriter,
                new WarmingQueue(warming.maximumPatterns()), popularityTracker, coverageIndex, createCanonicalizer(),
                createDataChangeDetector(), cubeCatalog,
                new WarmingConfiguration(settings.cacheExecution().bucketSeconds(), settings.warmingTopNQueries(),
                        warming.maximumPatterns(), warming.lookback(), applicationSettings.cacheSafety().maximumCachedRows(),
                        settings.cacheExecution().fixedStepSeconds()),
                Clock.systemUTC(), safetyConfiguration);
    }

    /**
     * The hot cache tier. Local runs use the in-memory blob backend, which stores the <em>same
     * serialized bytes</em> Valkey would — that fidelity is why a local run exercises the real
     * serialization path and not a shortcut.
     */
    private CacheBackendPort createCacheBackend() {
        ArrowResultFrameSerializer serializer = new ArrowResultFrameSerializer();
        return settings.cacheBackend() == CacheBackend.MEMORY
                ? new InMemoryBlobCacheBackendAdapter(serializer)
                : new ValkeyCacheBackendAdapter(settings.redisUri(), serializer, decodeTasks,
                        Math.min(2, mergeTasks.getCorePoolSize()), 64L * 1024 * 1024);
    }

    /**
     * Layer-2 popularity lives next to the buckets: in Valkey it survives restarts and idle weeks, so the
     * warmer keeps popular queries warm after a deploy. Local runs keep it in memory, like the buckets.
     */
    private QueryPopularityPort createPopularityTracker() {
        return settings.cacheBackend() == CacheBackend.MEMORY
                ? new QueryPopularityTracker(applicationSettings.warming().maximumPatterns())
                : new ValkeyQueryPopularityAdapter(settings.redisUri(), applicationSettings.warming().popularityRetention());
    }

    /** The data-change canary runs only when an operator supplies a validation query. */
    private DataChangeDetector createDataChangeDetector() {
        WarmingSettings warming = applicationSettings.warming();
        return warming.hasValidationSql()
                ? new DataChangeDetector(cacheBackend, createTranslator(), queryExecutor, warming.validationSql(),
                        warming.lookback(), settings.cacheExecution().timeColumnName())
                : null;
    }

    /** Uses configured column mappings; legacy settings retain the single identity-mapped table. */
    private TableCatalog createTableCatalog() {
        if (!applicationSettings.tables().isEmpty()) {
            TableCatalog catalog = new TableCatalog();
            applicationSettings.tables().forEach(table -> {
                RegisteredTable registered = new RegisteredTable(table.logicalName(), table.deltaPath(),
                        table.columnMappings(), table.timeColumns());
                if (!registered.physicalTimeColumns().stream()
                        .allMatch(settings.cacheExecution().timeColumnName()::equalsIgnoreCase))
                    throw new IllegalArgumentException("Table physical time columns must match the configured cache time column");
                catalog.register(registered);
            });
            return catalog;
        }
        return new TableCatalog().register(RegisteredTable.of(
                settings.mainTableName(),
                settings.mainTablePath(),
                Map.of(settings.cacheExecution().timeColumnName(), settings.cacheExecution().timeColumnName()),
                settings.cacheExecution().timeColumnName()));
    }

    /** Calcite canonicalisation, behind the cache's port. */
    private SqlCanonicalizerPort createCanonicalizer() {
        Set<String> batchPaths = applicationSettings.tables().isEmpty()
                ? Set.of(settings.mainTablePath())
                : applicationSettings.tables().stream().filter(table -> table.sourceType().equals("HOT_BATCH"))
                        .map(TableSettings::deltaPath).collect(Collectors.toSet());
        return new CalciteCanonicalObjectFactory(timeDimensions(), batchPaths,
                settings.cacheExecution().fixedStepSeconds());
    }

    /**
     * The physical time columns canonicalisation and gap rewriting both recognise: the configured cache
     * time column plus every registered table's mapped time columns.
     */
    private TimeDimensionMap timeDimensions() {
        Set<String> timeColumns = new HashSet<>(tableCatalog.physicalTimeColumns());
        timeColumns.add(settings.cacheExecution().timeColumnName());
        return new TimeDimensionMap(timeColumns);
    }

    /**
     * Logical→physical translation, behind the cache's port.
     *
     * <p>The bucket step handed to the translator is the <b>fixed internal step</b>, not the bucket
     * width. The translator emits {@code FLOOR(ts/step)*step} rows, the cache stores them at that same
     * step, and the merge resamples up to whatever the user asked for. Passing the bucket width here
     * instead would make the SQL produce day-granular rows while the merge expected step-granular ones.
     */
    private LogicalSqlTranslatorPort createTranslator() {
        return new LogicalToPhysicalSqlTranslator(settings.cacheExecution().fixedStepSeconds(), tableCatalog);
    }

    /** Gap-query rewriting, behind the cache's port. */
    private GapQueryRewriterPort createGapQueryRewriter() {
        return new GapQueryRewriterAdapter(timeDimensions(),
                settings.cacheExecution().timeColumnName(), settings.cacheExecution().bucketSeconds());
    }

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

    public CacheBackendPort cacheBackendForShutdown() {
        return cacheBackend;
    }

    private CacheConfiguration resolveCacheSafetyConfiguration() {
        long bucketSeconds = settings.cacheExecution().bucketSeconds();
        if (bucketSeconds % 3_600L != 0) {
            throw new IllegalArgumentException("CASCADA_BUCKET_SECONDS must be a whole number of hours");
        }
        CacheSafetySettings safety = applicationSettings.cacheSafety();
        return new CacheConfiguration(CacheConfiguration.defaults().impossibleAggregates(),
                safety.highCardinalityColumns(), safety.liquidClusteredFilterColumns(),
                settings.cacheExecution().fixedStepSeconds(), Math.toIntExact(bucketSeconds / 3_600L),
                Optional.ofNullable(safety.minimumCacheableTimeRangeSeconds()));
    }

    @Override
    public void close() {
        RuntimeException shutdownFailure = null;
        try {
            closeTasks(cacheTasks, "cache");
        } catch (RuntimeException failure) {
            shutdownFailure = failure;
        }
        try {
            closeTasks(decodeTasks, "decode");
        } catch (RuntimeException failure) {
            if (shutdownFailure == null) shutdownFailure = failure;
            else shutdownFailure.addSuppressed(failure);
        }
        try {
            closeTasks(mergeTasks, "merge");
        } catch (RuntimeException failure) {
            if (shutdownFailure == null) shutdownFailure = failure;
            else shutdownFailure.addSuppressed(failure);
        }
        for (Object connection : List.of(cacheBackend, popularityTracker)) {
            if (connection instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception failure) {
                    if (shutdownFailure == null) {
                        shutdownFailure = new IllegalStateException("failed to close cache connection", failure);
                    } else {
                        shutdownFailure.addSuppressed(failure);
                    }
                }
            }
        }
        if (shutdownFailure != null) throw shutdownFailure;
    }

    private void closeTasks(ExecutorService tasks, String name) {
        tasks.shutdown();
        try {
            if (!tasks.awaitTermination(30, TimeUnit.SECONDS)) {
                tasks.shutdownNow();
                throw new IllegalStateException(name + " tasks did not finish before shutdown");
            }
        } catch (InterruptedException interrupted) {
            tasks.shutdownNow();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while closing " + name + " tasks", interrupted);
        }
    }
}
