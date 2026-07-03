package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 0 — a query must contain at least one aggregate function to be cacheable (README caveat 7).
 *
 * <p>The whole bucket pyramid rests on one algebraic fact: partial results per bucket can be
 * <em>combined</em> (SUM of sums, MAX of maxima). A plain row-fetch {@code SELECT} has no combinable
 * measure at all — yet without this rule it passed every other guardrail, and the merge then
 * fabricated a {@code GROUP BY} + {@code SUM} the user never wrote, returning aggregated rows for a
 * query that asked for raw ones. Such queries must bypass to direct (Spark) execution, where they are
 * answered exactly as written.
 *
 * <p>Registered first: if there is nothing to aggregate, no other rule's opinion matters.
 */
public final class RequiresAggregationRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        if (canonicalObject.hashComponents().aggregates().isEmpty()) {
            return Optional.of(BypassReason.NO_AGGREGATION);
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "RequiresAggregationRule";
    }
}
