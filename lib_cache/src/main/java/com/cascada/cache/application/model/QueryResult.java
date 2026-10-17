package com.cascada.cache.application.model;

import com.cascada.cache.domain.frame.ResultFrame;

/**
 * The answer plus how it was obtained. {@code servedThroughCache == false} means a safety guardrail
 * forced a bypass and the physical SQL ran directly against the executor — the answer is still
 * correct, it just cost a full scan.
 */
public record QueryResult(ResultFrame frame, boolean servedThroughCache) {
}
