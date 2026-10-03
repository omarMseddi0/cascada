package com.cascada.spark.adapter.out.spark;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class SessionOwnershipCoordinatorTest {

    @Test
    void concurrentAcquisitionsDoNotBothClaimTheSameNewContext() throws Exception {
        SessionOwnershipCoordinator coordinator = new SessionOwnershipCoordinator();
        AtomicBoolean contextActive = new AtomicBoolean();
        AtomicInteger contextCreations = new AtomicInteger();
        Object sharedSession = new Object();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<SessionLease<Object>> first = pool.submit(() -> acquire(
                    coordinator, contextActive, contextCreations, sharedSession, ready, start));
            Future<SessionLease<Object>> second = pool.submit(() -> acquire(
                    coordinator, contextActive, contextCreations, sharedSession, ready, start));
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();

            SessionLease<Object> firstLease = first.get(5, TimeUnit.SECONDS);
            SessionLease<Object> secondLease = second.get(5, TimeUnit.SECONDS);

            assertThat(contextCreations).hasValue(1);
            assertThat(firstLease.session()).isSameAs(sharedSession);
            assertThat(secondLease.session()).isSameAs(sharedSession);
            assertThat(java.util.List.of(firstLease.ownsContext(), secondLease.ownsContext()))
                    .containsExactlyInAnyOrder(true, false);
        } finally {
            pool.shutdownNow();
        }
    }

    private SessionLease<Object> acquire(SessionOwnershipCoordinator coordinator, AtomicBoolean contextActive,
                                         AtomicInteger contextCreations, Object session,
                                         CountDownLatch ready, CountDownLatch start) throws InterruptedException {
        ready.countDown();
        start.await();
        return coordinator.acquire(contextActive::get, () -> {
            if (contextActive.compareAndSet(false, true)) {
                contextCreations.incrementAndGet();
            }
            return session;
        });
    }
}
