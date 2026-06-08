package com.cascada.cache.domain.safety;

import com.cascada.cache.domain.BypassReason;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Optional;

/**
 * RULE 4.5 — cached time-series are stored at the fixed internal step; exact reconstruction is only
 * possible when the user's requested step is an integer multiple of the fixed step. Ported from
 * {@code IncompatibleTimeStepRule} in {@code safety_rules.py}.
 *
 * <p>Decision table (identical to the Python):
 * <ul>
 *   <li>not a time series → no opinion;</li>
 *   <li>no user step and {@code preserveRawTimeSeries} → no opinion (raw buckets kept as-is);</li>
 *   <li>no user step and not preserving raw → bypass (cannot guarantee exact reconstruction);</li>
 *   <li>user step &lt; fixed step, or not a multiple of it → bypass.</li>
 * </ul>
 */
public final class IncompatibleTimeStepRule implements SafetyRule {

    @Override
    public Optional<BypassReason> evaluate(CanonicalQueryObject canonicalObject, CacheConfiguration configuration) {
        if (!canonicalObject.metadata().isTimeSeries()) {
            return Optional.empty();
        }

        int fixedStep = configuration.fixedStepSeconds();
        Optional<Integer> userStep = canonicalObject.metadata().userStepSeconds();

        if (userStep.isEmpty()) {
            return canonicalObject.metadata().preserveRawTimeSeries()
                    ? Optional.empty()
                    : Optional.of(BypassReason.INCOMPATIBLE_TIME_STEP);
        }

        int step = userStep.get();
        if (step < fixedStep || (step % fixedStep) != 0) {
            return Optional.of(BypassReason.INCOMPATIBLE_TIME_STEP);
        }
        return Optional.empty();
    }

    @Override
    public String ruleName() {
        return "IncompatibleTimeStepRule";
    }
}
