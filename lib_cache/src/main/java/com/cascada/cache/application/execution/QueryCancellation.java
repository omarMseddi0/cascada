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
    public CancellationRegistration onCancel(Runnable listener) {
        AtomicBoolean invoked = new AtomicBoolean();
        Runnable once = () -> { if (invoked.compareAndSet(false, true)) listener.run(); };
        listeners.add(once);
        if (isCancelled()) once.run();
        return new CancellationRegistration(() -> listeners.remove(once));
    }
}
