package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 0b — {@code DISTINCT}, {@code HAVING}, and {@code JOIN ... ON} are not mergeable across
 * buckets, so any query carrying one of them must bypass to direct (Spark) execution.
 *
 * <p>The bucket cache is only correct for aggregates whose per-bucket partials can be recombined
 * (SUM of sums, MAX of maxima). None of these three features has that property:
 * <ul>
 *   <li><b>DISTINCT</b> — the distinct set of one day plus the distinct set of the next day is not
 *       the distinct set of both days; rows present in both buckets would double-count, and per-bucket
 *       {@code COUNT(DISTINCT)} partials cannot be summed at all (holistic, plan Appendix I.2). Only
 *       a stored sketch (§8.13) can answer this from cache, and then only as approximate.</li>
 *   <li><b>HAVING</b> — it filters on the <em>final</em> aggregate value. Applying it per bucket
 *       filters partial sums (a group under the threshold on Monday may be over it for the week),
 *       and not applying it per bucket means the cached partials were never filtered at all.</li>
 *   <li><b>JOIN ... ON</b> — a join evaluated inside one time bucket misses every pair whose rows
 *       fall in different buckets; the per-bucket join results are not ingredients of the whole.</li>
 * </ul>
 *
 * <p>Hashing these features (the {@code logicSignature}) stops two different queries from colliding
 * on one cache entry; this rule is the second half of the fix — it stops the non-mergeable query from
 * being bucket-cached in the first place: when the merge algebra cannot represent the query, do not
 * cache, ever.
 */
public final class NonMergeableSqlFeatureRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        if (!canonicalObject.logicSignature().isEmpty()) {
            return Optional.of(BypassReason.NON_MERGEABLE_SQL_FEATURE);
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "NonMergeableSqlFeatureRule";
    }
}
