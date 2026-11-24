package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.application.model.query.ExactCubeTrace;
import com.cascada.cache.application.model.query.QueryResult;
import com.cascada.cache.application.port.out.ExactCubeSqlPort;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.application.port.out.PinnedQueryExecutorPort;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import java.util.logging.Logger;

/** Coordinates the opt-in, snapshot-bound route at logical SQL admission. */
public final class ExactCubeExecution {
    private static final Logger LOGGER = Logger.getLogger(ExactCubeExecution.class.getName());
    private static final int MAX_ENUMERATED_EDGES = 50_000;
    private static final int CACHE_MEMORY_UNITS = 64;
    private static final int MIB = 1 << 20;
    private static final int MAX_FETCH_BATCH = 4;
    private static final int MAX_FETCH_DECODED_BYTES = 8 * MIB;
    private static final Set<String> INTEGRALS = Set.of("tinyint", "smallint", "int");
    private final ExactCubeSqlPort sql;
    private final ExactCubeStorePort store;
    private final PinnedQueryExecutorPort executor;
    private final String accessScope;
    private final Clock clock;
    private final long bucketSeconds;
    private final Duration ttl;
    private final Semaphore cacheMemory = new Semaphore(CACHE_MEMORY_UNITS);

    public ExactCubeExecution(ExactCubeSqlPort sql, ExactCubeStorePort store, PinnedQueryExecutorPort executor,
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
        Optional<ExactCubeSource> binding;
        try {
            binding = executor.bind(query.path(), accessScope);
        } catch (Exception unavailable) {
            QueryCancellation.rethrowIfCancelled(unavailable);
            LOGGER.fine(() -> "Exact cube declined during snapshot admission: " + unavailable.getClass().getSimpleName());
            return existing("snapshot-admission-unavailable", fallback);
        }
        if (binding.isEmpty()) return existing("unbound-source", fallback);
        var source = binding.get();
        var population = new CubePopulation(source, query.timeColumn(), query.predicate(), query.input());
        String rejection = rejection(query, source);
        if (rejection != null) return direct(query, source, rejection, List.of(), List.of(), 0);
        return executeBound(query, population);
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
        for (var predicate : query.predicateTypes().entrySet()) {
            String type = source.types().get(predicate.getKey());
            String required = predicate.getValue();
            if (type == null || !required.isEmpty() && !required.equals(type)
                    && !(required.equals("integral") && (INTEGRALS.contains(type) || "bigint".equals(type)))) {
                return "unproven-predicate-type";
            }
        }
        if (!query.groupAliases().isEmpty() && (!source.groupByAliases()
                || query.groupAliases().stream().anyMatch(alias ->
                    source.types().keySet().stream().anyMatch(alias::equalsIgnoreCase)))) {
            return "unproven-group-alias";
        }
        if (query.grain().stream().anyMatch(name -> !ExactCubePlanChecker.validKeyType(source.types().get(name)))) {
            return "unproven-group-key-type";
        }
        return null;
    }

    private QueryResult executeBound(ExactCubeQuery query, CubePopulation population) {
        var source = population.source();
        var reads = new ArrayList<ExactCubeFragment>();
        var submitted = new ArrayList<String>();
        long planningStarted = System.nanoTime();
        long planningNanos = 0;
        String failureReason = null;
        try {
            var candidates = new ArrayList<>(store.candidates(population, query.interval(), query.grain(), query.measures()));
            if (candidates.isEmpty() && ExactCubePlanChecker.mayNarrow(query, population)) {
                var grain = new java.util.LinkedHashSet<>(query.grain());
                grain.addAll(query.predicateTypes().keySet());
                candidates.addAll(store.candidates(ExactCubePlanChecker.unfiltered(population), query.interval(),
                        List.copyOf(grain), query.measures()));
            }
            var missing = new ArrayList<ExactCubeFragment>(1);
            for (int attempt = 0; attempt < 2; attempt++) {
                var selected = ExactCubePlanner.select(query, population, candidates, bucketSeconds, MAX_ENUMERATED_EDGES);
                planningNanos += System.nanoTime() - planningStarted;
                if (selected.isEmpty()) break;
                int units = Math.toIntExact((decodedBytes(selected.get()) + MIB - 1) / MIB);
                if (!cacheMemory.tryAcquire(units)) {
                    failureReason = "cache-decode-budget";
                    break;
                }
                try {
                    var answer = compose(query, population, selected.get(), reads, submitted, missing, planningNanos);
                    if (answer != null) return answer;
                } finally {
                    cacheMemory.release(units);
                }
                candidates.removeAll(missing);
                planningStarted = System.nanoTime();
            }
            if (failureReason == null && !missing.isEmpty()) failureReason = "recovery:missing-generation";
            if (failureReason == null) {
                boolean alreadyPublished = candidates.stream().anyMatch(fragment ->
                        fragment.population().equals(population) && fragment.interval().equals(query.interval())
                                && fragment.grain().equals(query.grain())
                                && fragment.measures().containsAll(query.measures()));
                if (!alreadyPublished && query.outerSql() == null && query.limit() == null) {
                    return build(query, population, planningNanos);
                }
                failureReason = candidates.isEmpty() ? "no-manifest" : "no-beneficial-verified-cover";
            }
        } catch (Exception failure) {
            QueryCancellation.rethrowIfCancelled(failure);
            failureReason = "recovery:" + failure.getClass().getSimpleName();
            LOGGER.fine(() -> "Exact cube restarted at Delta version " + source.version() + ": " + failure.getMessage());
        }
        return direct(query, source, failureReason, reads, submitted, planningNanos);
    }

    private static long decodedBytes(ExactCubePlan plan) {
        return plan.covers().values().stream().flatMap(List::stream).map(edge -> edge.fragment())
                .filter(java.util.Objects::nonNull).distinct().mapToLong(ExactCubeFragment::decodedBytes).sum();
    }

    private QueryResult compose(ExactCubeQuery query, CubePopulation population, ExactCubePlan plan,
                                List<ExactCubeFragment> reads, List<String> submitted,
                                List<ExactCubeFragment> missing, long planningNanos) {
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
        var fragments = new ArrayList<>(cache.keySet());
        for (int offset = 0; offset < fragments.size();) {
            QueryCancellation.requireCurrentActive();
            int end = offset + 1;
            long batchBytes = fragments.get(offset).decodedBytes();
            while (end < fragments.size() && end - offset < MAX_FETCH_BATCH
                    && batchBytes + fragments.get(end).decodedBytes() <= MAX_FETCH_DECODED_BYTES) {
                batchBytes += fragments.get(end++).decodedBytes();
            }
            var batch = fragments.subList(offset, end);
            var frames = store.readBatch(batch);
            if (frames.size() != batch.size()) throw new IllegalStateException("invalid fragment batch size");
            int previousMisses = missing.size();
            for (int i = 0; i < batch.size(); i++) {
                if (frames.get(i).isPresent()) reads.add(batch.get(i));
                else missing.add(batch.get(i));
            }
            if (missing.size() != previousMisses) return null;
            for (int i = 0; i < batch.size(); i++) {
                var fragment = batch.get(i);
                var frame = frames.get(i);
                if (frame.get().rowCount() != fragment.rows()) throw new IllegalStateException("manifest row count mismatch");
                state.add(frame.get(), fragment.grain(), fragment.measures(), cache.get(fragment),
                        fragment.population().equals(population) ? List.of() : query.keyFilters());
            }
            offset = end;
        }
        var source = population.source();
        for (var entry : residuals.entrySet()) {
            String residual = sql.stateStatement(query, source, entry.getKey(), entry.getValue());
            submitted.add(residual);
            state.add(executor.executePinned(source, residual), query.grain(), entry.getValue(), entry.getValue());
        }
        var result = state.finish();
        String outer = null;
        if (query.outerSql() != null) {
            outer = sql.outerStatement(query, source);
            var types = query.projections().stream().map(projection -> switch (projection.operation()) {
                case KEY -> source.types().get(projection.column());
                case SUM, COUNT -> "bigint";
                case AVG -> "double";
            }).toList();
            result = executor.executeWithAggregate(source, outer, query.outerRelation(), result, types);
        }
        QueryCancellation.requireCurrentActive();
        return new QueryResult(result, true, trace("exact-cube", missing.isEmpty() ? "verified" : "verified-after-miss",
                source, reads, submitted, outer, planningNanos));
    }

    private QueryResult build(ExactCubeQuery query, CubePopulation population, long planningNanos) {
        String fence = store.publicationFence();
        var source = population.source();
        String statement = sql.stateStatement(query, source, query.interval(), query.measures());
        var frame = executor.executePinned(source, statement);
        var state = new ExactCubeState(query, source);
        state.add(frame, query.grain(), query.measures(), query.measures());
        var answer = state.finish();
        try {
            store.publish(population, query.interval(), query.grain(), query.measures(), frame, ttl, fence);
        } catch (Exception unavailable) {
            QueryCancellation.rethrowIfCancelled(unavailable);
            LOGGER.fine(() -> "Exact cube publication skipped: " + unavailable.getClass().getSimpleName());
        }
        return new QueryResult(answer, false, trace("pinned-build", "no-manifest", source, List.of(),
                List.of(), null, planningNanos));
    }

    private QueryResult direct(ExactCubeQuery query, ExactCubeSource source, String reason,
                               List<ExactCubeFragment> reads, List<String> submitted, long planningNanos) {
        String statement = sql.pinnedStatement(query, source);
        var result = executor.executePinned(source, statement);
        return new QueryResult(result, false, trace("pinned-spark", reason, source, reads, submitted, null, planningNanos));
    }

    private static ExactCubeTrace trace(String route, String reason, ExactCubeSource source,
                                         List<ExactCubeFragment> reads, List<String> submitted,
                                         String outer, long planningNanos) {
        LOGGER.fine(() -> "Exact cube route=" + route + " reason=" + reason + " version=" + source.version()
                + " generations=" + reads.stream().map(ExactCubeFragment::generation).toList()
                + " compressed_bytes=" + reads.stream().mapToLong(ExactCubeFragment::compressedBytes).sum()
                + " decoded_bytes=" + reads.stream().mapToLong(ExactCubeFragment::decodedBytes).sum()
                + " residuals=" + submitted.size() + " planning_ns=" + planningNanos);
        return new ExactCubeTrace(route, reason, source.version(), reads, submitted, outer, planningNanos);
    }
}
