package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.validation.ValidationBaseline;
import java.time.Instant;

record StoredBaseline(ValidationBaseline baseline, Instant expiresAt) {
}
