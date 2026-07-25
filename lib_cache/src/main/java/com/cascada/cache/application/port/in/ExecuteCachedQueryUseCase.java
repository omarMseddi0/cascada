package com.cascada.cache.application.port.in;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.frame.ResultFrame;

/**
 * <b>Primary (driving) port</b> — "answer this query, using the cache when it is safe to".
 *
 * <p>This is the hexagon's left-hand edge for the read path: every driving adapter (a REST controller,
 * a JDBC endpoint, a CLI, a test) speaks to the application through this interface and nothing else.
 * That is what makes the goal "the application should be equally controllable by users, other
 * applications, or automated tests" true rather than aspirational — the end-to-end wiring test and a
 * future REST controller call the identical method.
 *
 * <p>Implemented by {@code application.service.ExecuteCachedQueryService}. Driving adapters must depend
 * on this port, never on the service class: that rule is what lets the service be split, renamed, or
 * wrapped (metering, tracing, an RL policy) without touching a single adapter.
 */
public interface ExecuteCachedQueryUseCase {

    /**
     * Answer a canonicalised query.
     *
     * @param canonicalObject the deterministic description of query intent
     * @return the result frame plus whether it was served through the cache path
     */
    Result execute(CanonicalQueryObject canonicalObject);

    /**
     * The answer plus how it was obtained. {@code servedThroughCache == false} means a safety
     * guardrail forced a bypass and the physical SQL ran directly against the executor — the answer is
     * still correct, it just cost a full scan.
     */
    record Result(ResultFrame frame, boolean servedThroughCache) {
    }
}
