package com.cascada.cache.application.execution;

import java.util.Optional;
import java.util.function.Supplier;

/** Carries cancellation across request and cache-gap workers without transport dependencies. */
public final class QueryExecutionScope implements AutoCloseable {
    private static final ThreadLocal<QueryCancellation> CURRENT = new ThreadLocal<>();
    private final QueryCancellation previous;
    public QueryExecutionScope(QueryCancellation cancellation) {
        cancellation.requireActive();
        previous = CURRENT.get();
        CURRENT.set(cancellation);
    }
    public static Optional<QueryCancellation> current() { return Optional.ofNullable(CURRENT.get()); }
    public static <T> Supplier<T> propagate(Supplier<T> action) {
        return current().<Supplier<T>>map(cancellation -> () -> {
            try (var scope = new QueryExecutionScope(cancellation)) { return action.get(); }
        }).orElse(action);
    }
    public static Runnable propagate(Runnable action) {
        Supplier<Void> propagated = propagate(() -> { action.run(); return null; });
        return () -> propagated.get();
    }
    @Override public void close() {
        if (previous == null) CURRENT.remove(); else CURRENT.set(previous);
    }
}
