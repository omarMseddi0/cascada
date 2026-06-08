package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 3 — bypass when a filter is on a liquid-clustered column, because Delta data-skipping already
 * makes that query fast, so caching adds no value (and clustering should "win" over caching for the
 * same column — plan §8.10 de-duplication insight). Ported from {@code LiquidClusteredFilterRule}.
 *
 * <p>Match is a case-insensitive substring of the space-joined filter list, exactly as the Python
 * {@code col.lower() in query_filters_str.lower()}. The column set is auto-profiler output.
 */
public final class LiquidClusteredFilterRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        String joinedFilters = String.join(" ", canonicalObject.hashComponents().filters()).toLowerCase();
        for (String liquidClusteredColumn : configuration.liquidClusteredFilterColumns()) {
            if (joinedFilters.contains(liquidClusteredColumn.toLowerCase())) {
                return Optional.of(BypassReason.LIQUID_CLUSTERED_FILTER);
            }
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "LiquidClusteredFilterRule";
    }
}
