package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * A single guardrail that may force a cache bypass (ported from {@code ISafetyRule} in
 * {@code interfaces.py}). Renamed to the long-form port convention.
 *
 * <p>Safety rules are the floor of correctness: the RL policy may only choose <em>within</em> the
 * safe set they define, never against it (plan §8.6, §8.7). A rule returns the reason it fired, or
 * {@link Optional#empty()} to let evaluation continue to the next rule.
 */
public interface SafetyRule {

    Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration);

    /** A stable, human-readable name for diagnostics and the cost-preview explanation. */
    String ruleName();
}
