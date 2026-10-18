package com.cascada.cache.adapter.out.tracking.model;

import com.cascada.identity.domain.model.QueryHash;

public record RankedQuery(QueryHash query, long hits) { }
