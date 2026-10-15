package com.cascada.cache.domain.query;

import com.cascada.identity.domain.QueryHash;
import java.util.Objects;

/** A popular query family plus the physical SQL that rebuilds its canonical object after a restart. */
public record PopularQuery(QueryHash queryHash, String physicalSql) {
    public PopularQuery {
        Objects.requireNonNull(queryHash, "queryHash");
        Objects.requireNonNull(physicalSql, "physicalSql");
    }
}
