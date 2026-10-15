package com.cascada.cache.domain.validation;

import com.cascada.cache.domain.time.TimeRange;
import java.util.Objects;

/** The validation query's signature over a frozen window of complete historical buckets. */
public record ValidationBaseline(TimeRange window, String signature) {
    public ValidationBaseline {
        Objects.requireNonNull(window, "window");
        Objects.requireNonNull(signature, "signature");
    }
}
