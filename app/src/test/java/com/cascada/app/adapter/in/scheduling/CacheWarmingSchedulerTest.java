package com.cascada.app.adapter.in.scheduling;

import com.cascada.app.model.config.WarmingSettings;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.application.model.WarmingReport;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.identity.domain.model.QueryHash;
import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CacheWarmingSchedulerTest {
    @Test void runsTheLookbackCycleAtTheCurrentTimeAndSurvivesFailure() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch recovered = new CountDownLatch(1);
        Clock clock = Clock.fixed(Instant.ofEpochSecond(864000), ZoneOffset.UTC);
        WarmCacheUseCase warmer = new WarmCacheUseCase() {
            public void recordQuery(QueryHash hash, CanonicalQueryObject canonical) { }
            public WarmingReport warmCycle(long start, long end, boolean force) {
                throw new AssertionError("the scheduler must let the data-change canary decide, not force a window");
            }
            public WarmingReport warmLookbackWindow(long nowSeconds, Duration lookback) {
                assertThat(nowSeconds).isEqualTo(864000);
                assertThat(lookback).isEqualTo(Duration.ofDays(1));
                if (calls.incrementAndGet() == 1) throw new IllegalStateException("temporary outage");
                recovered.countDown(); return new WarmingReport(0, 0, 0);
            }
        };
        try (var scheduler = new CacheWarmingScheduler(warmer,
                new WarmingSettings(true, Duration.ofMillis(10), Duration.ofDays(1), 10), clock)) {
            scheduler.start(); assertThat(recovered.await(2, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(calls.get()).isGreaterThanOrEqualTo(2);
    }
}
