package com.cascada.cache.application.model;

import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.identity.domain.model.QueryHash;

/** Current-process identity for a persisted popularity observation. */
public record ResolvedWarmingQuery(QueryHash queryHash, CanonicalQueryObject canonical) {}
