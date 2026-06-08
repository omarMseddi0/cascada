package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 2 — bypass when GROUP BY touches a high-cardinality column, because caching a group-by on a
 * near-unique column would materialise roughly one row per value (huge memory, near-zero hit rate).
 * Ported from {@code HighCardinalityGroupByRule} in {@code safety_rules.py}.
 *
 * <p>Membership is exact (column equals a group-by entry), matching the Python {@code col in
 * query_group_by}. The crucial Cascada change is that the column set comes from the auto-profiler,
 * not a hand-written config (plan §8.10): the rule is identical, the data source is automatic.
 */
public final class HighCardinalityGroupByRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        for (String highCardinalityColumn : configuration.highCardinalityColumns()) {
            if (canonicalObject.hashComponents().groupBy().contains(highCardinalityColumn)) {
                return Optional.of(BypassReason.HIGH_CARDINALITY_GROUP_BY);
            }
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "HighCardinalityGroupByRule";
    }
}
