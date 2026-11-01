package com.cascada.cache.application.service.safety;

import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.safety.BypassReason;
import com.cascada.cache.domain.model.safety.CacheConfiguration;
import com.cascada.cache.domain.safety.CacheDecision;
import java.util.List;
import java.util.Optional;

/**
 * Orchestrates the safety rules with first-match-wins semantics, ported from {@code SafetyChecker}
 * in {@code safety_rules.py}. The first rule that fires returns its bypass reason; if none fire, the
 * query is safe to cache ({@link CacheDecision#AGGREGATION_V1}).
 *
 * Source eligibility is part of the safety contract: HOT_VIEW-only work bypasses bucket caching.
 */
public final class SafetyRuleRegistry {

    private final List<SafetyRule> rules;

    public SafetyRuleRegistry(List<SafetyRule> rules) {
        this.rules = List.copyOf(rules);
    }

    public static SafetyRuleRegistry defaultRegistry() {
        return new SafetyRuleRegistry(List.of(
                new RequiresAggregationRule(),
                new NonMergeableSqlFeatureRule(),
                new ImpossibleMathRule(),
                new HotViewOnlyRule(),
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

    /** Every rule that fires, in registration order — for decisions that depend on why a query bypassed. */
    public List<BypassReason> findAllBypassReasons(CanonicalQueryObject canonicalObject,
                                                   CacheConfiguration configuration) {
        return rules.stream()
                .flatMap(rule -> rule.evaluate(canonicalObject, configuration).stream())
                .toList();
    }

    /** Shared read-path and restart-warming policy; short ranges can warm complete future-use buckets. */
    public boolean permitsWarming(List<BypassReason> reasons, CacheConfiguration configuration) {
        return reasons.isEmpty() || reasons.contains(BypassReason.MINIMUM_TIME_RANGE)
                && reasons.stream().allMatch(reason -> reason == BypassReason.MINIMUM_TIME_RANGE
                        || reason == BypassReason.PARTIAL_DAY_BUCKET)
                && configuration.minimumCacheableTimeRangeSeconds().filter(seconds -> seconds > 0).isPresent();
    }

    public List<SafetyRule> rules() {
        return rules;
    }
}
