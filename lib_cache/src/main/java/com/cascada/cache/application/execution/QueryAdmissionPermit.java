package com.cascada.cache.application.execution;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/** A closeable permit returned by {@link QueryAdmissionBudget#acquire()}. */
public final class QueryAdmissionPermit implements AutoCloseable {

    private final Semaphore permits;
    private final AtomicBoolean closed = new AtomicBoolean();

    QueryAdmissionPermit(Semaphore permits) {
        this.permits = permits;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            permits.release();
        }
    }
}
