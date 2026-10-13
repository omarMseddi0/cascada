package com.cascada.cache.adapter.out.tracking;

import com.cascada.identity.domain.QueryHash;

record RankedQuery(QueryHash query, long hits) { }
