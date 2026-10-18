package com.cascada.cache.application.service;

import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.model.QueryResult;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.application.service.safety.SafetyRuleRegistry;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.safety.BypassReason;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.identity.domain.model.QueryHash;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * The application entry point ported from {@code cache_manager.py}'s handle-query flow: evaluate the
 * safety guardrails; on a bypass run the physical SQL directly on Spark; otherwise compute the logic
 * hash and run the {@link CacheExecutionEngine}.
 *
 * <p>The RL policy (plan §8.6) will later sit between the safety decision and the engine, but it can
 * only choose <em>within</em> the safe set this use case already enforces.
 */
public final class ExecuteCachedQueryService implements ExecuteCachedQueryUseCase {

    private static final Logger LOGGER = Logger.getLogger(ExecuteCachedQueryService.class.getName());
    /** A query bypassed only for being short still describes buckets a longer query will want. */
    private static final Set<BypassReason> SMALL_RANGE_REASONS =
            EnumSet.of(BypassReason.MINIMUM_TIME_RANGE, BypassReason.PARTIAL_DAY_BUCKET);

    private final SafetyRuleRegistry safetyRuleRegistry;
    private final CacheConfiguration cacheConfiguration;
    private final QueryHashGenerator queryHashGenerator;
    private final CacheExecutionEngine cacheExecutionEngine;
    private final QueryExecutorPort sparkExecutor;
    private final WarmCacheUseCase warmer;

    public ExecuteCachedQueryService(SafetyRuleRegistry safetyRuleRegistry, CacheConfiguration cacheConfiguration,
                                     QueryHashGenerator queryHashGenerator,
                                     CacheExecutionEngine cacheExecutionEngine,
                                     QueryExecutorPort sparkExecutor) {
        this(safetyRuleRegistry, cacheConfiguration, queryHashGenerator, cacheExecutionEngine, sparkExecutor, null);
    }

    public ExecuteCachedQueryService(SafetyRuleRegistry safetyRuleRegistry, CacheConfiguration cacheConfiguration,
                                     QueryHashGenerator queryHashGenerator,
                                     CacheExecutionEngine cacheExecutionEngine,
                                     QueryExecutorPort sparkExecutor, WarmCacheUseCase warmer) {
        this.safetyRuleRegistry = safetyRuleRegistry;
        this.cacheConfiguration = cacheConfiguration;
        this.queryHashGenerator = queryHashGenerator;
        this.cacheExecutionEngine = cacheExecutionEngine;
        this.sparkExecutor = sparkExecutor;
        this.warmer = warmer;
    }

    @Override
    public QueryResult execute(CanonicalQueryObject canonicalObject) {
        List<BypassReason> bypassReasons = safetyRuleRegistry.findAllBypassReasons(canonicalObject, cacheConfiguration);
        if (!bypassReasons.isEmpty()) {
            if (isWarmableSmallRange(bypassReasons)) {
                recordForWarming(canonicalObject);
            }
            return new QueryResult(sparkExecutor.execute(canonicalObject.physicalSql()), false);
        }
        QueryHash queryHash = recordForWarming(canonicalObject);
        return new QueryResult(cacheExecutionEngine.execute(canonicalObject, queryHash), true);
    }

    /**
     * Mirrors {@code _maybe_enqueue_small_range_for_warming}: the read path is skipped for a short window,
     * but warming builds full buckets the next, longer query can hit. A minimum of 0 or -1 disables the
     * cache entirely, so nothing is voted then.
     */
    private boolean isWarmableSmallRange(List<BypassReason> bypassReasons) {
        return bypassReasons.contains(BypassReason.MINIMUM_TIME_RANGE)
                && SMALL_RANGE_REASONS.containsAll(bypassReasons)
                && cacheConfiguration.minimumCacheableTimeRangeSeconds().filter(seconds -> seconds > 0).isPresent();
    }

    private QueryHash recordForWarming(CanonicalQueryObject canonicalObject) {
        QueryHash queryHash =
                queryHashGenerator.generateQueryHash(canonicalObject, cacheConfiguration.fixedStepSeconds());
        if (warmer != null) {
            try {
                warmer.recordQuery(queryHash, canonicalObject);
            } catch (RuntimeException observationFailure) {
                // Warming/popularity state is an optional hint. Its outage cannot break query service.
                LOGGER.warning("could not record warming observation for query hash " + queryHash + " ("
                        + observationFailure.getClass().getSimpleName() + ")");
            }
        }
        return queryHash;
    }
}
