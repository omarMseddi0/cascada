package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.application.model.query.ExactCubeTrace;
import com.cascada.cache.application.model.query.QueryResult;
import com.cascada.cache.application.port.out.ExactCubeSqlPort;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.application.port.out.LatestQueryExecutorPort;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.exception.CubeSourceChangedException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.logging.Logger;

/** Coordinates the opt-in exact cube route over the current Delta state. */
public final class ExactCubeExecution {
    private static final Logger LOGGER = Logger.getLogger(ExactCubeExecution.class.getName());
    private static final Set<String> INTEGRALS = Set.of("tinyint", "smallint", "int");
    private final ExactCubeSqlPort sql;
    private final ExactCubeStorePort store;
    private final LatestQueryExecutorPort executor;
    private final String accessScope;
    private final Clock clock;
    private final long bucketSeconds;
    private final Duration ttl;

    public ExactCubeExecution(ExactCubeSqlPort sql, ExactCubeStorePort store, LatestQueryExecutorPort executor,
                              String accessScope, Clock clock, long bucketSeconds, Duration ttl) {
        this.sql = sql;
        this.store = store;
        this.executor = executor;
        this.accessScope = accessScope;
        this.clock = clock;
        this.bucketSeconds = bucketSeconds;
        this.ttl = ttl;
    }

    public QueryResult execute(String physicalSql, Supplier<QueryResult> fallback) {
        var recognized = sql.recognize(physicalSql);
        if (recognized.isEmpty()) return existing("unproven-sql-shape", fallback);
        ExactCubeQuery query = recognized.get();
        Optional<ExactCubeSource> observed;
        try {
            observed = executor.currentSource(query.path(), accessScope);
        } catch (Exception unavailable) {
            QueryCancellation.rethrowIfCancelled(unavailable);
            LOGGER.fine(() -> "Exact cube declined during source metadata lookup: " + unavailable.getClass().getSimpleName());
            return existing("source-metadata-unavailable", fallback);
        }
        if (observed.isEmpty()) return existing("source-metadata-unavailable", fallback);
        var source = observed.get();
        var population = new CubePopulation(source, query.timeColumn(), query.predicate(), query.input());
        String rejection = rejection(query, source);
        if (rejection != null) return direct(physicalSql, rejection, List.of(), List.of(), 0);
        return executeObserved(physicalSql, query, population);
    }

    private static QueryResult existing(String reason, Supplier<QueryResult> fallback) {
        LOGGER.fine(() -> "Exact cube declined: " + reason);
        QueryResult result = fallback.get();
        return new QueryResult(result.frame(), result.servedThroughCache(), new ExactCubeTrace(
                "existing", reason, -1, List.of(), List.of(), null, 0));
    }

    private String rejection(ExactCubeQuery query, ExactCubeSource source) {
        var interval = query.interval();
        if (!ExactCubePlanChecker.aligned(interval, bucketSeconds)
                || interval.end() > clock.instant().getEpochSecond()) {
            return "open-or-unaligned-window";
        }
        if (!"bigint".equals(source.types().get(query.timeColumn()))
                || !INTEGRALS.contains(source.types().getOrDefault(query.input(), ""))) {
            return "unproven-integral-type";
        }
        if (query.grain().stream().anyMatch(name -> !ExactCubePlanChecker.validKeyType(source.types().get(name)))) {
            return "unproven-group-key-type";
        }
        return null;
    }

    private QueryResult executeObserved(String physicalSql, ExactCubeQuery query, CubePopulation population) {
        var reads = new ArrayList<ExactCubeFragment>();
        var submitted = new ArrayList<String>();
        long planningStarted = System.nanoTime();
        long planningNanos = 0;
        String failureReason;
        try {
            var candidates = store.candidates(population, query.interval());
            var selected = ExactCubePlanner.select(query, population, candidates, bucketSeconds);
            planningNanos = System.nanoTime() - planningStarted;
            if (selected.isPresent()) {
                return compose(query, population, selected.get(), reads, submitted, planningNanos);
            }
            boolean alreadyPublished = candidates.stream().anyMatch(fragment ->
                    fragment.interval().equals(query.interval()) && fragment.grain().equals(query.grain())
                            && fragment.measures().containsAll(query.measures()));
            if (!alreadyPublished && query.outerSql() == null && query.limit() == null) {
                return build(query, population, planningNanos);
            }
            failureReason = candidates.isEmpty() ? "no-manifest" : "no-beneficial-verified-cover";
        } catch (Exception failure) {
            QueryCancellation.rethrowIfCancelled(failure);
            failureReason = "recovery:" + failure.getClass().getSimpleName();
            LOGGER.fine(() -> "Exact cube restarted against latest Delta data: " + failure.getMessage());
        }
        return direct(physicalSql, failureReason, reads, submitted, planningNanos);
    }

    private QueryResult compose(ExactCubeQuery query, CubePopulation population, ExactCubePlan plan,
                                List<ExactCubeFragment> reads, List<String> submitted, long planningNanos) {
        if (!ExactCubePlanChecker.valid(query, population, plan, bucketSeconds)) {
            throw new IllegalStateException("invalid cube cover");
        }
        var state = new ExactCubeState(query, population.source());
        var cache = new LinkedHashMap<ExactCubeFragment, EnumSet<CubeMeasure>>();
        var residuals = new LinkedHashMap<CubeInterval, EnumSet<CubeMeasure>>();
        for (CubeMeasure measure : query.measures().stream().sorted().toList()) {
            for (var edge : plan.covers().get(measure)) {
                if (edge.fragment() == null) {
                    residuals.computeIfAbsent(edge.interval(), ignored -> EnumSet.noneOf(CubeMeasure.class)).add(measure);
                } else {
                    cache.computeIfAbsent(edge.fragment(), ignored -> EnumSet.noneOf(CubeMeasure.class)).add(measure);
                }
            }
        }
        for (var entry : cache.entrySet()) {
            var fragment = entry.getKey();
            QueryCancellation.requireCurrentActive();
            var frame = store.read(fragment).orElseThrow(() ->
                    new IllegalStateException("missing or corrupt generation " + fragment.generation()));
            reads.add(fragment);
            if (frame.rowCount() != fragment.rows()) throw new IllegalStateException("manifest row count mismatch");
            state.add(frame, fragment.grain(), fragment.measures(), entry.getValue());
        }
        var source = population.source();
        for (var entry : residuals.entrySet()) {
            String residual = sql.stateStatement(query, source, entry.getKey(), entry.getValue());
            submitted.add(residual);
            state.add(executor.execute(residual), query.grain(), entry.getValue(), entry.getValue());
        }
        requireCurrentSource(source);
        var result = state.finish();
        String outer = null;
        if (query.outerSql() != null) {
            outer = sql.outerStatement(query, source, result);
            result = executor.execute(outer);
            requireCurrentSource(source);
        }
        return new QueryResult(result, true, trace("exact-cube", "verified", source.version(), reads, submitted, outer, planningNanos));
    }

    private QueryResult build(ExactCubeQuery query, CubePopulation population, long planningNanos) {
        String fence = store.publicationFence();
        var source = population.source();
        String statement = sql.stateStatement(query, source, query.interval(), query.measures());
        var frame = executor.execute(statement);
        var state = new ExactCubeState(query, source);
        state.add(frame, query.grain(), query.measures(), query.measures());
        var answer = state.finish();
        requireCurrentSource(source);
        try {
            store.publish(population, query.interval(), query.grain(), query.measures(), frame, ttl, fence);
        } catch (Exception unavailable) {
            QueryCancellation.rethrowIfCancelled(unavailable);
            LOGGER.fine(() -> "Exact cube publication skipped: " + unavailable.getClass().getSimpleName());
        }
        return new QueryResult(answer, false, trace("latest-build", "no-manifest", source.version(), List.of(),
                List.of(), null, planningNanos));
    }

    private void requireCurrentSource(ExactCubeSource observed) {
        QueryCancellation.requireCurrentActive();
        if (executor.currentSource(observed.path(), observed.accessScope()).filter(observed::equals).isEmpty()) {
            throw new CubeSourceChangedException();
        }
    }

    private QueryResult direct(String physicalSql, String reason, List<ExactCubeFragment> reads,
                               List<String> submitted, long planningNanos) {
        var result = executor.execute(physicalSql);
        return new QueryResult(result, false, trace("latest-spark", reason, -1, reads, submitted, null, planningNanos));
    }

    private static ExactCubeTrace trace(String route, String reason, long observedVersion,
                                         List<ExactCubeFragment> reads, List<String> submitted,
                                         String outer, long planningNanos) {
        LOGGER.fine(() -> "Exact cube route=" + route + " reason=" + reason + " version=" + observedVersion
                + " generations=" + reads.stream().map(ExactCubeFragment::generation).toList()
                + " compressed_bytes=" + reads.stream().mapToLong(ExactCubeFragment::compressedBytes).sum()
                + " decoded_bytes=" + reads.stream().mapToLong(ExactCubeFragment::decodedBytes).sum()
                + " residuals=" + submitted.size() + " planning_ns=" + planningNanos);
        return new ExactCubeTrace(route, reason, observedVersion, reads, submitted, outer, planningNanos);
    }
}
