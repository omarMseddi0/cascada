package com.cascada.cache.application.execution;

import com.cascada.cache.application.model.CancellationRegistration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A shared, fair limit on the number of cache queries admitted at once.
 *
 * <p>This is a query-count budget, not a byte budget: result sizes are unknown until decoded and
 * merge accumulator state depends on the data. Callers should share one instance across engines
 * whose query-local cache and merge state compete for the same process resources.
 */
public final class QueryAdmissionBudget {

    private final int maximumConcurrentQueries;
    private final Semaphore permits;

    public QueryAdmissionBudget(int maximumConcurrentQueries) {
        if (maximumConcurrentQueries <= 0) {
            throw new IllegalArgumentException("maximumConcurrentQueries must be > 0");
        }
        this.maximumConcurrentQueries = maximumConcurrentQueries;
        this.permits = new Semaphore(maximumConcurrentQueries, true);
    }

    public int maximumConcurrentQueries() {
        return maximumConcurrentQueries;
    }

    /**
     * Waits fairly for admission. Request cancellation interrupts only the waiting thread so it can
     * leave the semaphore queue promptly; the returned permit must be closed after the admitted
     * query releases its query-local state.
     */
    public QueryAdmissionPermit acquire() {
        QueryCancellation.requireCurrentActive();
        Thread waitingThread = Thread.currentThread();
        Object cancellationHandoff = new Object();
        AtomicBoolean waiting = new AtomicBoolean(true);
        CancellationRegistration cancellationRegistration = QueryExecutionScope.current()
                .map(cancellation -> cancellation.onCancel(() -> {
                    synchronized (cancellationHandoff) {
                        if (waiting.get()) waitingThread.interrupt();
                    }
                }))
                .orElse(null);
        boolean acquired = false;
        boolean transferred = false;
        try {
            QueryCancellation.requireCurrentActive();
            permits.acquire();
            acquired = true;

            // Synchronize with a cancellation callback already in flight before disabling its
            // interrupt. The active check below observes any cancellation that won the handoff.
            synchronized (cancellationHandoff) {
                waiting.set(false);
            }
            if (cancellationRegistration != null) {
                cancellationRegistration.close();
                cancellationRegistration = null;
            }
            QueryCancellation.requireCurrentActive();
            QueryAdmissionPermit admission = new QueryAdmissionPermit(permits);
            transferred = true;
            return admission;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new CancellationException("Query was interrupted while waiting for admission");
        } finally {
            if (cancellationRegistration != null) cancellationRegistration.close();
            if (acquired && !transferred) permits.release();
        }
    }
}
