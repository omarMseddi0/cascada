package com.cascada.cache.application.service.safety;

import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.safety.BypassReason;
import com.cascada.cache.domain.safety.CacheConfiguration;
import java.util.Locale;
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

    private static final String APPROXIMATE_DISTINCT_COUNT = "APPROX_COUNT_DISTINCT(";

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        // APPROX_COUNT_DISTINCT is the sketch path: its partials are mergeable HLL sketches, so the
        // DISTINCT inside its name must not be read as an exact COUNT(DISTINCT ...).
        String joinedAggregates = String.join(" ", canonicalObject.hashComponents().aggregates())
                .toUpperCase(Locale.ROOT).replace(APPROXIMATE_DISTINCT_COUNT, "");
        for (String impossibleAggregate : configuration.impossibleAggregates()) {
            if (joinedAggregates.contains(impossibleAggregate.toUpperCase(Locale.ROOT))) {
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
