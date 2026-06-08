package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 1 — bypass aggregates that cannot be reconstructed by summing day-buckets
 * (e.g. {@code COUNT(DISTINCT)}, {@code MEDIAN}, {@code PERCENTILE}). Ported from
 * {@code ImpossibleMathRule} in {@code safety_rules.py}.
 *
 * <p>Match is a case-insensitive substring test over the space-joined aggregate list, exactly as
 * the Python {@code impossible_agg.upper() in query_aggs_str.upper()}. Cascada later relaxes this
 * for the sketch path (HLL/KLL, plan §8.13), but exact-math bypass remains the safe default.
 */
public final class ImpossibleMathRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        String joinedAggregates = String.join(" ", canonicalObject.hashComponents().aggregates()).toUpperCase();
        for (String impossibleAggregate : configuration.impossibleAggregates()) {
            if (joinedAggregates.contains(impossibleAggregate.toUpperCase())) {
                return Optional.of(BypassReason.IMPOSSIBLE_MATH);
            }
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "ImpossibleMathRule";
    }
}
