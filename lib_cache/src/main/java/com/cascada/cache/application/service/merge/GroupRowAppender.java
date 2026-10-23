package com.cascada.cache.application.service.merge;

import com.cascada.cache.domain.frame.ResultFrameBuilder;

@FunctionalInterface
interface GroupRowAppender {
    void append(ResultFrameBuilder output, TypedAggregationGroups groups, int group);
}
