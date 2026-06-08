package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 4.6 — a configurable floor on the query duration that may be cached. Ported from
 * {@code MinTimerangeRule} in {@code safety_rules.py}.
 *
 * <p>Semantics (identical to the Python): absent config → no opinion; value {@code 0} or {@code -1}
 * disables caching entirely → bypass; a positive value bypasses queries whose duration is shorter
 * than it (those still get enqueued for warming so later, larger queries hit the cache).
 */
public final class MinimumTimeRangeRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        Optional<Long> configured = configuration.minimumCacheableTimeRangeSeconds();
        if (configured.isEmpty()) {
            return Optional.empty();
        }

        long minimumRange = configured.get();
        if (minimumRange == 0 || minimumRange == -1) {
            return Optional.of(BypassReason.MINIMUM_TIME_RANGE);
        }

        if (minimumRange > 0) {
            long duration = canonicalObject.timeRange().durationSeconds();
            if (duration < minimumRange) {
                return Optional.of(BypassReason.MINIMUM_TIME_RANGE);
            }
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "MinimumTimeRangeRule";
    }
}
