package com.cascada.cache.application.service.cube;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.model.query.ExactCubePlan;
import com.cascada.cache.application.model.query.BucketCubeBinding;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.merge.AverageIngredients;
import com.cascada.cache.domain.model.cube.CubeOperation;
import java.util.EnumMap;
import java.util.concurrent.Executor;
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
import java.util.function.Supplier;
import java.util.logging.Logger;

/** Coordinates the opt-in, snapshot-bound route at logical SQL admission. */
public final class ExactCubeExecution {
    private static final Logger LOGGER = Logger.getLogger(ExactCubeExecution.class.getName());
    private static final Set<String> INTEGRALS = Set.of("tinyint", "smallint", "int");
    private final ExactCubeSqlPort sql;
    private final ExactCubeStorePort store;
    private final PinnedQueryExecutorPort executor;
    private final String accessScope;
    private final Clock clock;
    private final long bucketSeconds;
    private final int mergeWorkers;
    private final Executor mergeExecutor;

    public ExactCubeExecution(ExactCubeSqlPort sql, ExactCubeStorePort store, PinnedQueryExecutorPort executor,
                              String accessScope, Clock clock, long bucketSeconds, int mergeWorkers, Executor mergeExecutor) {
        this.sql = sql;
        this.store = store;
        this.executor = executor;
        this.accessScope = accessScope;
        this.clock = clock;
        this.bucketSeconds = bucketSeconds;
        this.mergeWorkers = mergeWorkers;
        this.mergeExecutor = mergeExecutor;
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
        return executeBound(query, population, fallback);
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

    private QueryResult executeBound(ExactCubeQuery query, CubePopulation population, Supplier<QueryResult> existingRoute) {
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
            var selected = ExactCubePlanner.select(query, population, candidates, bucketSeconds);
            planningNanos = System.nanoTime() - planningStarted;
            if (selected.isPresent()) {
                return compose(query, population, selected.get(), reads, submitted, planningNanos);
            }
            failureReason = candidates.isEmpty() ? "no-manifest" : "no-beneficial-verified-cover";
        } catch (Exception failure) {
            QueryCancellation.rethrowIfCancelled(failure);
            failureReason = "recovery:" + failure.getClass().getSimpleName();
            LOGGER.fine(() -> "Exact cube restarted at Delta version " + source.version() + ": " + failure.getMessage());
            return direct(query, source, failureReason, reads, submitted, planningNanos);
        }
        return existing(failureReason, existingRoute);
    }

    private QueryResult compose(ExactCubeQuery query, CubePopulation population, ExactCubePlan plan,
                                List<ExactCubeFragment> reads, List<String> submitted, long planningNanos) {
        if (!ExactCubePlanChecker.valid(query, population, plan, bucketSeconds)) {
            throw new IllegalStateException("invalid cube cover");
        }
        var state = new ExactCubeState(query, population.source(), mergeWorkers, mergeExecutor);
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
        var fragments = List.copyOf(cache.keySet());
        store.visitBuckets(fragments, (index, value) -> {
            var fragment = fragments.get(index);
            var frame = value.orElseThrow(() ->
                    new IllegalStateException("missing or changed bucket " + fragment.bucketKey()));
            reads.add(fragment);
            if (frame.rowCount() != fragment.rows()) throw new IllegalStateException("bucket row count mismatch");
            String alias = fragment.averageAlias();
            var columns = new EnumMap<CubeMeasure, String>(CubeMeasure.class);
            for (CubeMeasure measure : cache.get(fragment)) {
                columns.put(measure, measure == CubeMeasure.SUM
                        ? AverageIngredients.sumColumn(alias) : AverageIngredients.countColumn(alias));
            }
            state.add(frame, fragment.grain(), fragment.grain(), AverageIngredients.countColumn(alias), columns,
                    fragment.population().equals(population) ? List.of() : query.keyFilters());
        });
        var source = population.source();
        for (var entry : residuals.entrySet()) {
            String residual = sql.stateStatement(query, source, entry.getKey(), entry.getValue());
            submitted.add(residual);
            var columns = new EnumMap<CubeMeasure, String>(CubeMeasure.class);
            for (CubeMeasure measure : entry.getValue()) columns.put(measure, measure.stateLabel());
            var keys = java.util.stream.IntStream.range(0, query.grain().size()).mapToObj(i -> "cube_k" + i).toList();
            state.add(executor.executePinned(source, residual), query.grain(), keys, "cube_rows", columns, List.of());
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
        return new QueryResult(result, true, trace("exact-cube", "verified", source, reads, submitted, outer, planningNanos));
    }

    /** Only ordinary AVG buckets with the original grouping columns can be indexed. */
    public Optional<BucketCubeBinding> bindWarmBucket(String physicalSql) {
        var recognized = sql.recognize(physicalSql);
        if (recognized.isEmpty()) return Optional.empty();
        var query = recognized.get();
        if (query.outerSql() != null || !query.groupAliases().isEmpty()) return Optional.empty();
        var averages = query.projections().stream().filter(p -> p.operation() == CubeOperation.AVG).toList();
        if (averages.size() != 1 || !query.grain().stream().allMatch(key -> query.projections().stream()
                .anyMatch(p -> p.operation() == CubeOperation.KEY && p.column().equals(key) && p.label().equals(key)))) {
            return Optional.empty();
        }
        return executor.bind(query.path(), accessScope).filter(source -> rejection(query, source) == null)
                .map(source -> new BucketCubeBinding(
                        new CubePopulation(source, query.timeColumn(), query.predicate(), query.input()),
                        query.grain(), averages.getFirst().label()));
    }

    public ResultFrame executeWarmBucket(String bucketSql, BucketCubeBinding binding) {
        var source = binding.population().source();
        return executor.executePinned(source, sql.pinnedBucketStatement(bucketSql, source));
    }

    public boolean storeWarmBucket(String key, ResultFrame frame, Duration ttl, String generation,
                                   long start, long end, BucketCubeBinding binding) {
        return store.storeBucket(key, frame, ttl, generation, binding.population(),
                new CubeInterval(start, Math.incrementExact(end)), binding.grain(), binding.averageAlias());
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
