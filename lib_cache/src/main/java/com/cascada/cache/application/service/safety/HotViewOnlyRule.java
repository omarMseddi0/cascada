package com.cascada.cache.application.service.safety;

import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.safety.BypassReason;
import com.cascada.cache.domain.safety.CacheConfiguration;
import java.util.Optional;

/** Pre-aggregated view-only queries do not enter the raw-data bucket cache. */
public final class HotViewOnlyRule implements SafetyRule {
    @Override public Optional<BypassReason> evaluate(CanonicalQueryObject query, CacheConfiguration configuration) {
        return query.metadata().usesHotBatch() ? Optional.empty() : Optional.of(BypassReason.HOT_VIEW_ONLY);
    }
}
