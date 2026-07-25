package com.cascada.app.bootstrap;

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

import java.util.Map;
import java.util.Objects;

/**
 * <b>The composition root.</b> This is the only class in the entire codebase permitted to know which
 * concrete adapter satisfies which port, and it contains no business logic whatsoever — every method
 * either instantiates one object or hands ports to a service.
 *
 * <p>Why that restriction is the whole point: because the wiring is confined here, "local" and "cluster"
 * differ only in which two or three objects this factory constructs. The cache engine, the safety rules,
 * the merge math, and the SQL translation are byte-identical in both. Nothing downstream can behave
 * differently, because nothing downstream knows.
 *
 * <p>Read it as three bands, outermost last:
 * <ol>
 *   <li><b>Outbound adapters</b> — the plugs: executor, cache backend, coverage index, SQL parser.</li>
 *   <li><b>Application services</b> — constructed from ports only.</li>
 *   <li><b>Inbound ports</b> — returned to the caller as interfaces, never as service classes, so a
 *       driving adapter cannot accidentally bind itself to an implementation.</li>
 * </ol>
 *
 * <p>This is deliberately hand-written wiring rather than annotation scanning. It is a plain
 * {@code new}-expression graph: no reflection, no proxies, no classpath magic, and a compile error the
 * moment a required port is missing. If Spring is ever introduced, the equivalent shape is a
 * {@code @Configuration} class with one {@code @Bean} method per method below — the article's
 * recommendation — so that the framework stays outside the core rather than being scattered through it.
 *
 * <h2>Not yet wired</h2>
 * The pieces below are designed but unimplemented; each has a TODO at its seam. They are listed here
 * because this is the file that will change when they land:
 * <ul>
 *   <li>tenant resolution — every port is tenant-ready (keys embed the tenant segment) but nothing
 *       populates a {@code TenantIdentifier} yet, so everything runs as the implicit default tenant;</li>
 *   <li>a Valkey-backed {@code CoverageIndexPort} (only the in-memory twin exists);</li>
 *   <li>a durable {@code QueryPopularityPort} (the in-memory tracker loses counts on restart);</li>
 *   <li>multi-table catalogs (exactly one table is registered from settings);</li>
 *   <li>the auto-profiler that is supposed to fill {@code CacheConfiguration}'s high-cardinality and
 *       liquid-clustered column sets — they are currently always empty, so those two safety rules can
 *       never fire.</li>
 * </ul>
 */
public final class CascadaEngineFactory {

    private final EngineSettings settings;

    // Outbound adapters, created once and shared by every service below.
    private final QueryExecutorPort queryExecutor;
    private final CacheBackendPort cacheBackend;
    private final CoverageIndexPort coverageIndex;
    private final QueryPopularityPort popularityTracker;
    private final TableCatalog tableCatalog;

    /**
     * @param queryExecutor the execution tier. Passed in rather than built here because it is the one
     *     adapter whose construction needs a runtime that may be absent (a Spark session); a test injects
     *     a fake, {@code CascadaLauncher} injects the real Spark/Delta adapter.
     */
    public CascadaEngineFactory(EngineSettings settings, QueryExecutorPort queryExecutor) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.queryExecutor = Objects.requireNonNull(queryExecutor, "queryExecutor");
        this.cacheBackend = cacheBackend();
        this.coverageIndex = coverageIndex();
        this.popularityTracker = popularityTracker();
        this.tableCatalog = tableCatalog();
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
        return settings.useLocalSpark()
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
     * <p>TODO(cascada): load every table from deployment configuration and scope the catalog per tenant.
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
        return new CalciteCanonicalObjectFactory();
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
        CacheExecutionEngine executionEngine = new CacheExecutionEngine(
                cacheBackend, queryExecutor, gapQueryRewriter(), settings.cacheExecution(),
                coverageIndex, new CubeShapeCatalog());
        return new ExecuteCachedQueryService(
                SafetyRuleRegistry.defaultRegistry(),
                // TODO(cascada): the auto-profiler must supply highCardinalityColumns and
                // liquidClusteredFilterColumns here. While both sets are empty,
                // HighCardinalityGroupByRule and LiquidClusteredFilterRule can never fire, so a
                // group-by on a near-unique column will be cached instead of bypassed.
                CacheConfiguration.defaults(),
                new QueryHashGenerator(),
                executionEngine,
                queryExecutor);
    }

    /** The read path for logical SQL — what a REST or JDBC adapter should drive. */
    public ExecuteLogicalQueryUseCase executeLogicalQueryUseCase() {
        return new ExecuteLogicalQueryService(translator(), canonicalizer(), executeCachedQueryUseCase());
    }

    /** The administrator console's size measurement. */
    public MeasureCacheSizeUseCase measureCacheSizeUseCase() {
        return new CacheAdministrationService(cacheBackend);
    }

    /** The administrator console's flush action. */
    public FlushCacheUseCase flushCacheUseCase() {
        return new CacheAdministrationService(cacheBackend);
    }

    /**
     * The warmer.
     *
     * <p>TODO(cascada): nothing calls {@link WarmCacheUseCase#recordQuery} yet, so the Layer-1 queue is
     * always empty and warming only ever considers Layer-2 popularity. The read path needs to notify the
     * warmer on each query — cleanly done as a decorator around
     * {@link #executeCachedQueryUseCase()} rather than by giving the cache engine a dependency on the
     * warmer, which would couple the read path to a background concern.
     *
     * <p>TODO(cascada): no scheduler adapter exists to call {@link WarmCacheUseCase#warmCycle}. It needs
     * a driving adapter (a scheduled executor, or a Kubernetes CronJob invoking a CLI subcommand).
     */
    public WarmCacheUseCase warmCacheUseCase() {
        return new WarmingOrchestrator(
                cacheBackend, queryExecutor, gapQueryRewriter(), new WarmingQueue(), popularityTracker,
                coverageIndex, settings.cacheExecution().bucketSeconds(), settings.warmingTopNQueries());
    }

    /** Exposed so a launcher can close adapters that hold sockets or sessions. */
    public CacheBackendPort cacheBackendForShutdown() {
        return cacheBackend;
    }
}
