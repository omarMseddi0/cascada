package com.cascada.cache.application.result;

import com.cascada.cache.domain.frame.ResultFrame;

public record QueryResult(ResultFrame frame, boolean servedThroughCache) {
}
