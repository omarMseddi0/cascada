package com.cascada.app.concurrent;

import com.cascada.app.config.ServerSettings;
import java.util.concurrent.*;

/** One independent virtual thread for each request; Spark owns execution and result-size configuration. */
public final class RequestExecutor implements AutoCloseable {
    private final ExecutorService threads = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("query-request-", 0).factory());
    private final ServerSettings settings;
    public RequestExecutor(ServerSettings settings) { this.settings = settings; }
    public Future<?> submit(Runnable operation) { return threads.submit(operation); }
    @Override public void close() {
        threads.shutdownNow();
        try { threads.awaitTermination(settings.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
}
