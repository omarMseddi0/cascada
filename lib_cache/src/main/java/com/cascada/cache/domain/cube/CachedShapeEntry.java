package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ResultFrame;

/**
 * A catalogued cache entry available for subsumption: its {@link QueryShape} and the materialised
 * (coarser) frame that a finer query may be answered from by rolling up and filtering down.
 */
public record CachedShapeEntry(QueryShape shape, ResultFrame frame) {
}
