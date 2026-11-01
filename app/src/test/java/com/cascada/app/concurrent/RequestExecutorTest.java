package com.cascada.app.concurrent;

import com.cascada.app.model.config.ServerSettings;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RequestExecutorTest {
    @Test void independentRequestsRunWithoutAConcurrencyGateAndCanBeCancelled() throws Exception {
        CountDownLatch entered = new CountDownLatch(2), release = new CountDownLatch(1);
        try (var executor = new RequestExecutor(new ServerSettings(0, 0, Duration.ofSeconds(2)))) {
            Runnable operation = () -> {
                assertThat(Thread.currentThread().isVirtual()).isTrue(); entered.countDown();
                try { release.await(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            };
            var first = executor.submit(operation); var second = executor.submit(operation);
            assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); first.cancel(true);
            release.countDown(); second.get(2, TimeUnit.SECONDS);
            executor.submit(() -> assertThat(Thread.currentThread().isVirtual()).isTrue()).get(2, TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }

}
