package com.cascada.cache.application.model.warming;

import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.util.Optional;
import java.util.Set;

/** Proposed baseline, acknowledged only after a successful warming cycle. */
public record ValidationCheck(boolean changed, Optional<ValidationBaseline> nextBaseline,
                              Set<Long> changedBuckets) {
    public ValidationCheck {
        changedBuckets = Set.copyOf(changedBuckets);
    }

    public boolean requiresFullRefresh() {
        return changed && changedBuckets.isEmpty();
    }

    public ValidationCheck(boolean changed, Optional<ValidationBaseline> nextBaseline) {
        this(changed, nextBaseline, Set.of());
    }
}
