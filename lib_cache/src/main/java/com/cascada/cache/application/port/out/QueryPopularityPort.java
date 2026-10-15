package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.query.PopularQuery;
import com.cascada.identity.domain.QueryHash;
import java.util.List;

/**
 * Outbound (driven) port for the Layer-2 warming feed: how often each query logic hash has been observed,
 * the top-N most popular hashes, and the physical SQL needed to warm them again.
 *
 * <p>The SQL is stored with the count because a durable implementation outlives the in-memory
 * hash&nbsp;&rarr;&nbsp;canonical registry: after a restart the warmer re-canonicalises the SQL, so popular
 * queries keep being warmed even when nobody has queried since the deploy.
 *
 * <p>Semantics an implementation must honour:
 * <ul>
 *   <li>{@link #recordObservation} is monotonic — counts never decrease while a query is retained;</li>
 *   <li>{@link #topByPopularity} returns at most {@code limit} queries, highest count first;</li>
 *   <li>a {@code limit} of {@code 0} returns an empty list (the warmer disables Layer 2 that way).</li>
 * </ul>
 */
public interface QueryPopularityPort {

    /** Record one observation of a query (a submission or a cache hit), incrementing its counter. */
    void recordObservation(QueryHash queryHash, String physicalSql);

    /** The cumulative observation count for a hash; {@code 0} when never seen. */
    long hitCount(QueryHash queryHash);

    /** The most popular {@code limit} queries, highest count first. */
    List<PopularQuery> topByPopularity(int limit);
}
