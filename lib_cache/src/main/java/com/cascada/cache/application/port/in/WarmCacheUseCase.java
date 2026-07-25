package com.cascada.cache.application.port.in;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.identity.domain.QueryHash;

/**
 * <b>Primary (driving) port</b> — pre-compute buckets a query is likely to want, so the read path finds
 * them already cached.
 *
 * <p>It is driven from two directions, which is why both methods live on one port:
 * <ul>
 *   <li>{@link #recordQuery} is called by the <em>read path</em> on every query — the "vote" that makes
 *       a just-missed pattern a warming candidate;</li>
 *   <li>{@link #warmCycle} is called by a <em>scheduler</em> adapter (a cron trigger, a background
 *       thread, a Kubernetes CronJob) to drain those votes and warm the window.</li>
 * </ul>
 *
 * <p>Invariant a warmer must never break: only <b>complete</b> buckets are stored. A bucket key claims
 * the full span {@code [start, start + bucketSeconds - 1]}; storing a frame truncated mid-bucket under
 * that key would undercount permanently, because the presence check would then treat the hole as
 * filled and never recompute it.
 */
public interface WarmCacheUseCase {

    /** Read-path hook: register the query so the warmer can rebuild its SQL, and vote for it. */
    void recordQuery(QueryHash queryHash, CanonicalQueryObject canonicalObject);

    /**
     * Run one warming cycle over the inclusive window.
     *
     * @param forceOverwrite re-warm buckets that already exist (used when a data-change signal says the
     *     underlying rows moved); normally {@code false}, which skips already-cached buckets
     */
    Report warmCycle(long warmStartTimestampSeconds, long warmEndTimestampSeconds, boolean forceOverwrite);

    /** Per-cycle totals, for the operator log and the warming dashboard. */
    record Report(int patternsWarmed, int bucketsWarmed, int bucketsSkipped) {
    }
}
