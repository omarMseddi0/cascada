package com.cascada.cache.application.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryAdmissionBudgetTest {

    @Test
    void admitsConfiguredParallelWorkAndQueuesTheNextQuery() throws Exception {
        QueryAdmissionBudget budget = new QueryAdmissionBudget(2);
        ExecutorService requests = Executors.newFixedThreadPool(3);
        CountDownLatch firstTwoAdmitted = new CountDownLatch(2);
        CountDownLatch releaseQueries = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int index = 0; index < 3; index++) {
                results.add(requests.submit(() -> {
                    try (var ignored = budget.acquire()) {
                        int current = active.incrementAndGet();
                        maximumActive.accumulateAndGet(current, Math::max);
                        firstTwoAdmitted.countDown();
                        assertThat(releaseQueries.await(5, TimeUnit.SECONDS)).isTrue();
                        active.decrementAndGet();
                    }
                    return null;
                }));
            }

            assertThat(firstTwoAdmitted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(active.get()).isEqualTo(2);
            assertThat(maximumActive.get()).isEqualTo(2);
            releaseQueries.countDown();
            for (Future<?> result : results) result.get(5, TimeUnit.SECONDS);
            assertThat(active.get()).isZero();
            assertThat(maximumActive.get()).isEqualTo(2);
        } finally {
            releaseQueries.countDown();
            requests.shutdownNow();
        }
    }

    @Test
    void cancellationWhileWaitingDoesNotConsumeOrLeakAPermit() throws Exception {
        QueryAdmissionBudget budget = new QueryAdmissionBudget(1);
        QueryCancellation cancellation = new QueryCancellation();
        ExecutorService requests = Executors.newSingleThreadExecutor();
        CountDownLatch waitingStarted = new CountDownLatch(1);
        AtomicReference<Thread> waitingThread = new AtomicReference<>();
        try (var held = budget.acquire()) {
            Future<?> waiting = requests.submit(() -> {
                waitingThread.set(Thread.currentThread());
                try (var scope = new QueryExecutionScope(cancellation)) {
                    waitingStarted.countDown();
                    try (var ignored = budget.acquire()) {
                        throw new AssertionError("cancelled waiter must not be admitted");
                    }
                }
            });

            assertThat(waitingStarted.await(5, TimeUnit.SECONDS)).isTrue();
            awaitState(waitingThread.get(), Thread.State.WAITING);
            cancellation.cancel();
            assertThatThrownBy(() -> waiting.get(1, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(CancellationException.class);
        } finally {
            requests.shutdownNow();
        }

        try (var acquiredAfterCancellation = budget.acquire()) {
            assertThat(acquiredAfterCancellation).isNotNull();
        }
    }

    @Test
    void cancellationRacingPermitReleaseNeverInterruptsAnAdmittedQuery() throws Exception {
        QueryAdmissionBudget budget = new QueryAdmissionBudget(1);
        ExecutorService requests = Executors.newSingleThreadExecutor();
        try {
            for (int attempt = 0; attempt < 40; attempt++) {
                QueryCancellation cancellation = new QueryCancellation();
                CountDownLatch waitingStarted = new CountDownLatch(1);
                AtomicReference<Thread> waitingThread = new AtomicReference<>();
                try (var held = budget.acquire()) {
                    Future<Boolean> result = requests.submit(() -> {
                        waitingThread.set(Thread.currentThread());
                        try (var scope = new QueryExecutionScope(cancellation)) {
                            waitingStarted.countDown();
                            try (var ignored = budget.acquire()) {
                                return !Thread.currentThread().isInterrupted();
                            }
                        } catch (CancellationException cancelled) {
                            // Clear the restored interruption before this pooled thread is reused.
                            Thread.interrupted();
                            return true;
                        }
                    });
                    assertThat(waitingStarted.await(5, TimeUnit.SECONDS)).isTrue();
                    awaitState(waitingThread.get(), Thread.State.WAITING);

                    CountDownLatch raceStart = new CountDownLatch(1);
                    Thread canceller = new Thread(() -> {
                        try {
                            raceStart.await();
                            cancellation.cancel();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    });
                    canceller.start();
                    raceStart.countDown();
                    held.close();
                    assertThat(result.get(1, TimeUnit.SECONDS)).isTrue();
                    canceller.join(1000);
                    assertThat(canceller.isAlive()).isFalse();
                }
            }
        } finally {
            requests.shutdownNow();
        }
    }

    @Test
    void fairAdmissionLetsAnEarlierWaiterProceedBeforeALaterOne() throws Exception {
        QueryAdmissionBudget budget = new QueryAdmissionBudget(1);
        ExecutorService requests = Executors.newFixedThreadPool(2);
        CountDownLatch firstWaiting = new CountDownLatch(1);
        CountDownLatch secondWaiting = new CountDownLatch(1);
        CountDownLatch firstAdmitted = new CountDownLatch(1);
        CountDownLatch letFirstRelease = new CountDownLatch(1);
        CountDownLatch secondAdmitted = new CountDownLatch(1);
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        AtomicReference<Thread> firstThread = new AtomicReference<>();
        AtomicReference<Thread> secondThread = new AtomicReference<>();
        Future<?> first;
        Future<?> second;
        try (var held = budget.acquire()) {
            first = requests.submit(() -> {
                firstThread.set(Thread.currentThread());
                firstWaiting.countDown();
                try (var ignored = budget.acquire()) {
                    order.add(1);
                    firstAdmitted.countDown();
                    assertThat(letFirstRelease.await(5, TimeUnit.SECONDS)).isTrue();
                }
                return null;
            });
            assertThat(firstWaiting.await(5, TimeUnit.SECONDS)).isTrue();
            awaitState(firstThread.get(), Thread.State.WAITING);

            second = requests.submit(() -> {
                secondThread.set(Thread.currentThread());
                secondWaiting.countDown();
                try (var ignored = budget.acquire()) {
                    order.add(2);
                    secondAdmitted.countDown();
                }
                return null;
            });
            assertThat(secondWaiting.await(5, TimeUnit.SECONDS)).isTrue();
            awaitState(secondThread.get(), Thread.State.WAITING);
        }

        try {
            assertThat(firstAdmitted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(secondAdmitted.getCount()).isEqualTo(1);
            letFirstRelease.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertThat(order).containsExactly(1, 2);
        } finally {
            letFirstRelease.countDown();
            requests.shutdownNow();
        }
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatThrownBy(() -> new QueryAdmissionBudget(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void awaitState(Thread thread, Thread.State expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.getState() != expected && System.nanoTime() < deadline) {
            Thread.sleep(5);
        }
        assertThat(thread.getState()).isEqualTo(expected);
    }
}
