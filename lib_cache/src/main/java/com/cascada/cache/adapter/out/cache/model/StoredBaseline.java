package com.cascada.cache.adapter.out.cache.model;

import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.time.Instant;

public record StoredBaseline(ValidationBaseline baseline, Instant expiresAt) {
}
