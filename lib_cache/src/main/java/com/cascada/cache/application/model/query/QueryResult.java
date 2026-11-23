package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.frame.ResultFrame;

/** The answer and how it was obtained. The exact-cube trace is internal to the query use case. */
public record QueryResult(ResultFrame frame, boolean servedThroughCache, ExactCubeTrace exactCubeTrace) {
    public QueryResult(ResultFrame frame, boolean servedThroughCache) {
        this(frame, servedThroughCache, null);
    }
}
