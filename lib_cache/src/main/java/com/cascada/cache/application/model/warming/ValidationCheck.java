package com.cascada.cache.application.model.warming;

import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.util.Optional;

/** Proposed baseline, acknowledged only after a successful warming cycle. */
public record ValidationCheck(boolean changed, Optional<ValidationBaseline> nextBaseline) { }
