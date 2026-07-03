package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CacheDecision;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.List;
import java.util.Optional;

/**
 * Orchestrates the safety rules with first-match-wins semantics, ported from {@code SafetyChecker}
 * in {@code safety_rules.py}. The first rule that fires returns its bypass reason; if none fire, the
 * query is safe to cache ({@link CacheDecision#AGGREGATION_V1}).
 *
 * <p>The {@code Open/Closed} principle (ARCHITECTURE §C.2): you extend the safe set by registering a
 * new {@link SafetyRule}, never by editing the engine. {@link #defaultRegistry()} is the generalised
 * Cascada set. The reference engine's microservice-specific "hot view only" rule is intentionally
 * absent (plan §8.11): the customer never selects a caching mode, so nothing bypasses on it.
 */
public final class SafetyRuleRegistry {

    private final List<SafetyRule> rules;

    public SafetyRuleRegistry(List<SafetyRule> rules) {
        this.rules = List.copyOf(rules);
    }

    public static SafetyRuleRegistry defaultRegistry() {
        return new SafetyRuleRegistry(List.of(
                new RequiresAggregationRule(),
                new ImpossibleMathRule(),
                new HighCardinalityGroupByRule(),
                new LiquidClusteredFilterRule(),
                new IncompatibleTimeStepRule(),
                new MinimumTimeRangeRule(),
                new PartialDayBucketRule()));
    }

    /** Mirrors {@code SafetyChecker.evaluate}: {@code V4_BYPASS} on first match, else {@code V1_AGGREGATION}. */
    public CacheDecision evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        return findFirstBypassReason(canonicalObject, configuration)
                .map(BypassReason::asCacheDecision)
                .orElse(CacheDecision.AGGREGATION_V1);
    }

    /** The richer form: which rule (if any) forced the bypass, for the cost-preview explanation. */
    public Optional<BypassReason> findFirstBypassReason(CanonicalQueryObject canonicalObject,
                                                        CacheConfiguration configuration) {
        for (SafetyRule rule : rules) {
            Optional<BypassReason> reason = rule.evaluate(canonicalObject, configuration);
            if (reason.isPresent()) {
                return reason;
            }
        }
        return Optional.empty();
    }

    public List<SafetyRule> rules() {
        return rules;
    }
}
