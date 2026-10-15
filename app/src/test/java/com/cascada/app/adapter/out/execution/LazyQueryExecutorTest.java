package com.cascada.app.adapter.out.execution;

import com.cascada.app.bootstrap.CascadaLauncher;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import com.cascada.cache.domain.frame.ResultFrame;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LazyQueryExecutorTest {
    @Test
    void activeQueriesCanExecuteConcurrently() throws Exception {
        CountDownLatch entered = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger starts = new AtomicInteger();
        LazyQueryExecutor executor = new LazyQueryExecutor(() -> {
            starts.incrementAndGet();
            return sql -> {
                entered.countDown();
                try {
                    if (!finish.await(5, TimeUnit.SECONDS)) throw new AssertionError("query release timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return ResultFrame.builder().build();
            };
        });
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> executor.execute("SELECT 1"));
            var second = workers.submit(() -> executor.execute("SELECT 2"));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            finish.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertThat(starts).hasValue(1);
        } finally {
            finish.countDown();
            workers.shutdownNow();
            executor.close();
        }
    }

    @Test
    void shutdownWaitsForAnActiveQueryBeforeClosingItsResources() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicInteger stops = new AtomicInteger();
        class BlockingExecutor implements com.cascada.cache.application.port.out.QueryExecutorPort, AutoCloseable {
            public ResultFrame execute(String sql) {
                entered.countDown();
                try {
                    if (!finish.await(5, TimeUnit.SECONDS)) throw new AssertionError("query release timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                assertThat(stops).hasValue(0);
                return ResultFrame.builder().build();
            }
            public void close() { stops.incrementAndGet(); }
        }
        LazyQueryExecutor executor = new LazyQueryExecutor(BlockingExecutor::new);
        var workers = Executors.newFixedThreadPool(2);
        try {
            var query = workers.submit(() -> executor.execute("SELECT 1"));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            var shutdown = workers.submit(() -> { executor.close(); return null; });
            finish.countDown();
            query.get(5, TimeUnit.SECONDS);
            shutdown.get(5, TimeUnit.SECONDS);
            assertThat(stops).hasValue(1);
            assertThatThrownBy(() -> executor.execute("SELECT 2")).isInstanceOf(IllegalStateException.class);
        } finally {
            finish.countDown();
            workers.shutdownNow();
            executor.close();
        }
    }
    @Test
    void administrativeLifecycleDoesNotStartExecutionTier() throws Exception {
        AtomicInteger starts = new AtomicInteger();
        LazyQueryExecutor executor = new LazyQueryExecutor(() -> {
            starts.incrementAndGet();
            throw new AssertionError("administration must not start Spark");
        });
        executor.close();
        assertThat(starts).hasValue(0);
        assertThatThrownBy(() -> executor.execute("SELECT 1")).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void queriesReuseTheExecutorAndCloseItOnce() throws Exception {
        AtomicInteger starts = new AtomicInteger();
        AtomicInteger stops = new AtomicInteger();
        LazyQueryExecutor executor = new LazyQueryExecutor(() -> {
            starts.incrementAndGet();
            return new CloseTrackingQueryExecutor(stops);
        });
        executor.execute("SELECT 1");
        executor.execute("SELECT 2");
        executor.close();
        executor.close();
        assertThat(starts).hasValue(1);
        assertThat(stops).hasValue(1);
    }

    @Test
    void usageDoesNotInitializeConfiguredRuntime() throws Exception {
        CascadaLauncher.main(new String[0]);
        CascadaLauncher.main(new String[]{"query"});
    }
}
