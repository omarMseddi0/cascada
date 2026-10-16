package com.cascada.app.bootstrap;

import com.cascada.app.concurrent.RequestExecutor;
import com.cascada.app.config.ServerSettings;
import com.cascada.cache.application.execution.*;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import java.time.Duration;
import java.util.concurrent.*;
import org.apache.spark.scheduler.*;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.*;

@EnabledIfSystemProperty(named="cascada.spark.integration", matches="true")
class SharedSparkSessionIntegrationTest {
    @Test void fiftyTwoVirtualRequestsUseOneRealSparkSessionWithIndependentJobGroups() throws Exception {
        SparkSession session = SparkSession.builder().master("local[4]").appName("cascada-shared-session-java25")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1").config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        java.util.Set<String> observedGroups = ConcurrentHashMap.newKeySet();
        session.sparkContext().addSparkListener(new SparkListener() {
            @Override public void onJobStart(SparkListenerJobStart event) {
                String group = event.properties().getProperty("spark.jobGroup.id");
                if (group != null) observedGroups.add(group);
            }
        });
        try (var executor = new SparkDeltaQueryExecutor(session);
             var requests = new RequestExecutor(new com.cascada.app.config.ServerSettings(0, 0, Duration.ofSeconds(10)))) {
            session.sparkContext().setLogLevel("ERROR");
            CountDownLatch entered = new CountDownLatch(52), release = new CountDownLatch(1);
            java.util.List<Future<?>> pending = new java.util.ArrayList<>();
            for (int requestIndex = 0; requestIndex < 52; requestIndex++) {
                int expectedOffset = requestIndex;
                pending.add(requests.submit(() -> {
                    assertThat(Thread.currentThread().isVirtual()).isTrue(); entered.countDown();
                    try { release.await(10, TimeUnit.SECONDS); }
                    catch (InterruptedException interrupted) { throw new java.util.concurrent.CancellationException(); }
                    try (var scope = new QueryExecutionScope(new QueryCancellation())) {
                        var frame = executor.execute("SELECT CAST(SUM(id) AS BIGINT) + " + expectedOffset + " AS total FROM range(1000)");
                        assertThat(frame.longAt(0, 0)).isEqualTo(499500L + expectedOffset);
                    }
                    assertThat(session.sparkContext().getLocalProperty("spark.jobGroup.id")).isNull();
                }));
            }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue(); release.countDown();
            for (var request : pending) request.get(60, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (observedGroups.size() < 52 && System.nanoTime() < deadline) Thread.sleep(10);
            assertThat(observedGroups).hasSize(52);
            assertThat(session.sparkContext().isStopped()).isFalse();
        } finally { session.stop(); }
    }
}
