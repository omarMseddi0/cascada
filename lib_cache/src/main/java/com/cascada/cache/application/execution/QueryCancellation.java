package com.cascada.cache.application.execution;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class QueryCancellation {
    private final String executionId = UUID.randomUUID().toString();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final java.util.Set<Runnable> listeners = ConcurrentHashMap.newKeySet();
    public String executionId() { return executionId; }
    public boolean isCancelled() { return cancelled.get(); }
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            listeners.forEach(listener -> {
                try { listener.run(); }
                catch (RuntimeException failure) {
                    java.util.logging.Logger.getLogger(QueryCancellation.class.getName())
                            .log(java.util.logging.Level.WARNING, "Query cancellation listener failed", failure);
                }
            });
        }
    }
    public void requireActive() {
        if (isCancelled() || Thread.currentThread().isInterrupted()) throw new CancellationException("Query was cancelled");
    }
    /** The calling thread's work is still wanted: not interrupted, and its query scope, if any, not cancelled. */
    public static void requireCurrentActive() {
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Query was interrupted");
        QueryExecutionScope.current().ifPresent(QueryCancellation::requireActive);
    }
    /**
     * Lets a cancellation pass through code that otherwise recovers from a failure: rethrows a
     * {@link CancellationException} found anywhere in {@code failure}'s cause chain, turns an
     * {@link InterruptedException} cause into one (restoring the interrupt), and also fails when the
     * current query was cancelled meanwhile. Returns normally for an ordinary failure.
     */
    public static void rethrowIfCancelled(Throwable failure) {
        requireCurrentActive();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof CancellationException cancelled) throw cancelled;
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw new CancellationException("Query was interrupted");
            }
        }
    }
    public CancellationRegistration onCancel(Runnable listener) {
        AtomicBoolean invoked = new AtomicBoolean();
        Runnable once = () -> { if (invoked.compareAndSet(false, true)) listener.run(); };
        listeners.add(once);
        if (isCancelled()) once.run();
        return new CancellationRegistration(() -> listeners.remove(once));
    }
}
