package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.time.Duration;
import java.util.Optional;

/** Durable home of the data-change canary's last accepted signature. */
public interface ValidationBaselinePort {

    /** The stored baseline, or empty on first run, after expiry, or when the stored value is unreadable. */
    Optional<ValidationBaseline> loadValidationBaseline();

    void saveValidationBaseline(ValidationBaseline baseline, Duration timeToLive);
}
