package com.cascada.app.concurrent;

import com.cascada.app.model.config.ServerSettings;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import java.util.Set;
import java.util.concurrent.*;

/**
 * One independent virtual thread for each request; Spark owns execution and result-size configuration.
 * Every transport submits through {@link #submit(QueryCancellation, Runnable, Runnable)}, so each request
 * runs inside its cancellation scope and {@link #close()} cancels whatever is still running.
 */
public final class RequestExecutor implements AutoCloseable {
    private final ExecutorService threads = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("query-request-", 0).factory());
    private final Set<QueryCancellation> active = ConcurrentHashMap.newKeySet();
    private final ServerSettings settings;
    public RequestExecutor(ServerSettings settings) { this.settings = settings; }

    public Future<?> submit(Runnable operation) { return threads.submit(operation); }

    /**
     * Runs {@code operation} on its own virtual thread inside {@code cancellation}'s query scope. When the
     * request was cancelled before its thread started, {@code cancelledBeforeStart} runs instead, so the
     * transport can still release its exchange or close its call.
     *
     * @throws RejectedExecutionException when the executor is shutting down
     */
    public Future<?> submit(QueryCancellation cancellation, Runnable operation, Runnable cancelledBeforeStart) {
        return threads.submit(() -> {
            active.add(cancellation);
            try {
                QueryExecutionScope scope;
                try {
                    scope = new QueryExecutionScope(cancellation);
                } catch (CancellationException cancelled) {
                    cancelledBeforeStart.run();
                    return;
                }
                try (scope) {
                    operation.run();
                }
            } finally {
                active.remove(cancellation);
            }
        });
    }

    @Override public void close() {
        active.forEach(QueryCancellation::cancel);
        threads.shutdownNow();
        try { threads.awaitTermination(settings.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
}
