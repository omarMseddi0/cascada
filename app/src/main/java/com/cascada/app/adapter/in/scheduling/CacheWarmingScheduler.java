package com.cascada.app.adapter.in.scheduling;

import com.cascada.app.config.WarmingSettings;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import java.time.Clock;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public final class CacheWarmingScheduler implements AutoCloseable {
    private final WarmCacheUseCase warmer;
    private final WarmingSettings settings;
    private final Clock clock;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("cache-warming-", 0).factory());
    private final AtomicReference<QueryCancellation> active = new AtomicReference<>();
    public CacheWarmingScheduler(WarmCacheUseCase warmer, WarmingSettings settings, Clock clock) {
        this.warmer = warmer; this.settings = settings; this.clock = clock;
    }
    public void start() {
        if (settings.enabled()) scheduler.scheduleWithFixedDelay(this::warmRecentQueries,
                settings.interval().toMillis(), settings.interval().toMillis(), TimeUnit.MILLISECONDS);
    }
    void warmRecentQueries() {
        QueryCancellation cancellation = new QueryCancellation();
        active.set(cancellation);
        try (var scope = new QueryExecutionScope(cancellation)) {
            warmer.warmLookbackWindow(clock.instant().getEpochSecond(), settings.lookback());
        } catch (RuntimeException failure) {
            if (!cancellation.isCancelled()) java.util.logging.Logger.getLogger(getClass().getName())
                    .warning("Cache warming cycle failed: " + failure.getClass().getSimpleName());
        } finally { active.compareAndSet(cancellation, null); }
    }
    @Override public void close() {
        java.util.Optional.ofNullable(active.get()).ifPresent(QueryCancellation::cancel);
        scheduler.shutdownNow();
        try { scheduler.awaitTermination(30, TimeUnit.SECONDS); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
    }
}
