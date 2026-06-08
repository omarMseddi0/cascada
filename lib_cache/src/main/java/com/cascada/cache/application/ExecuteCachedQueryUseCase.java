package com.cascada.cache.application;

import com.cascada.cache.domain.CacheDecision;
import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.hashing.QueryHashGenerator;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;
import com.cascada.cache.domain.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.SafetyRuleRegistry;
import com.cascada.identity.domain.QueryHash;

/**
 * The application entry point ported from {@code cache_manager.py}'s handle-query flow: evaluate the
 * safety guardrails; on a bypass run the physical SQL directly on Spark; otherwise compute the logic
 * hash and run the {@link CacheExecutionEngine}.
 *
 * <p>The RL policy (plan §8.6) will later sit between the safety decision and the engine, but it can
 * only choose <em>within</em> the safe set this use case already enforces.
 */
public final class ExecuteCachedQueryUseCase {

    private final SafetyRuleRegistry safetyRuleRegistry;
    private final CacheConfiguration cacheConfiguration;
    private final QueryHashGenerator queryHashGenerator;
    private final CacheExecutionEngine cacheExecutionEngine;
    private final SparkQueryExecutorPort sparkExecutor;

    public ExecuteCachedQueryUseCase(SafetyRuleRegistry safetyRuleRegistry, CacheConfiguration cacheConfiguration,
                                     QueryHashGenerator queryHashGenerator,
                                     CacheExecutionEngine cacheExecutionEngine,
                                     SparkQueryExecutorPort sparkExecutor) {
        this.safetyRuleRegistry = safetyRuleRegistry;
        this.cacheConfiguration = cacheConfiguration;
        this.queryHashGenerator = queryHashGenerator;
        this.cacheExecutionEngine = cacheExecutionEngine;
        this.sparkExecutor = sparkExecutor;
    }

    public Result execute(CanonicalQueryObject canonicalObject) {
        CacheDecision decision = safetyRuleRegistry.evaluate(canonicalObject, cacheConfiguration);
        if (decision.isBypass()) {
            return new Result(sparkExecutor.execute(canonicalObject.physicalSql()), false);
        }
        QueryHash queryHash =
                queryHashGenerator.generateQueryHash(canonicalObject, cacheConfiguration.fixedStepSeconds());
        return new Result(cacheExecutionEngine.execute(canonicalObject, queryHash), true);
    }

    /** The answer plus whether it went through the cache path (false = safety bypass to Spark). */
    public record Result(ResultFrame frame, boolean servedThroughCache) {
    }
}
