package com.cascada.cache.application.execution;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueryExecutionScopeTest {
    @Test
    void failingListenerDoesNotPreventOtherCancellationCallbacks() {
        QueryCancellation cancellation = new QueryCancellation();
        AtomicInteger healthy = new AtomicInteger();
        cancellation.onCancel(() -> { throw new IllegalStateException("listener failed"); });
        cancellation.onCancel(healthy::incrementAndGet);
        assertThatCode(cancellation::cancel).doesNotThrowAnyException();
        cancellation.cancel();
        assertThat(healthy).hasValue(1);
        assertThat(cancellation.isCancelled()).isTrue();
    }
    @Test void propagatesAndRestoresScopeAcrossReusedWorkers() throws Exception {
        QueryCancellation cancellation = new QueryCancellation();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            try (var scope = new QueryExecutionScope(cancellation)) {
                var action = QueryExecutionScope.propagate(() -> QueryExecutionScope.current().orElseThrow());
                assertThat(worker.submit(action::get).get()).isSameAs(cancellation);
            }
            assertThat(worker.submit(() -> QueryExecutionScope.current().isEmpty()).get()).isTrue();
            cancellation.cancel();
            assertThatThrownBy(() -> new QueryExecutionScope(cancellation)).isInstanceOf(CancellationException.class);
            assertThat(QueryExecutionScope.current()).isEmpty();
        } finally { worker.shutdownNow(); }
    }
    @Test void cancellationCallbacksAreOnceOnlyAndUnregisteredOnClose() {
        QueryCancellation cancellation = new QueryCancellation();
        AtomicInteger invocations = new AtomicInteger();
        var removed = cancellation.onCancel(invocations::incrementAndGet); removed.close();
        try (var registration = cancellation.onCancel(invocations::incrementAndGet)) {
            cancellation.cancel(); cancellation.cancel(); assertThat(invocations).hasValue(1);
        }
        try (var late = cancellation.onCancel(invocations::incrementAndGet)) { assertThat(invocations).hasValue(2); }
    }
}
