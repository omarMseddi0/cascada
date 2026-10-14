package com.cascada.app.bootstrap;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.config.*;
import com.cascada.app.config.ApplicationSettings;
import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.config.EngineSettings;
import com.cascada.app.config.ServerSettings;
import com.cascada.app.config.TableSettings;
import com.cascada.app.config.WarmingSettings;
import com.cascada.cache.application.config.CacheExecutionConfiguration;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.fabric.adapter.out.manifest.ClusterManifestRenderer;
import com.cascada.fabric.application.port.out.ClusterOrchestratorPort;
import com.cascada.fabric.application.service.ClusterLifecycleService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ApplicationIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static HttpResponse<String> post(HttpClient client, int port, String path, String body) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }
    @Test void drivesTheExistingLibraryGraphThroughBothTransports() throws Exception {
        AtomicInteger executions = new AtomicInteger();
        List<String> clusterActions = new ArrayList<>();
        ClusterOrchestratorPort orchestrator = new ClusterOrchestratorPort() {
            public List<String> apply(List<String> documents) {
                assertThat(documents).hasSize(10);
                assertThat(String.join("\n", documents)).contains("org.apache.gluten.GlutenPlugin");
                clusterActions.add("deploy"); return List.of("Deployment/cascada-driver-copy1");
            }
            public void delete(List<String> documents) { clusterActions.add("delete"); }
            public void scaleWorkload(String namespace, String name, int replicas) { clusterActions.add("scale=" + replicas); }
            public void restartWorkload(String namespace, String name) { clusterActions.add("restart"); }
        };
        var settings = new EngineSettings(CacheExecutionConfiguration.defaults(), "redis://unused:6379", "traffic", "/tmp/traffic", true, 10);
        var application = new ApplicationSettings(new ServerSettings(0, 0, Duration.ofSeconds(2)),
                new WarmingSettings(true, Duration.ofHours(1), Duration.ofDays(7), 32),
                List.of(new TableSettings("traffic", "/tmp/traffic", Map.of("ts", "ts", "bytes", "C6"), Set.of("ts"))));
        try (var runtime = new CascadaRuntime(settings, () -> sql -> {
            assertThat(sql).contains("delta.`/tmp/traffic`").contains("C6"); executions.incrementAndGet();
            return ResultFrame.builder().column("n", ColumnType.DOUBLE).row(10.0).build();
        }, application, new ClusterLifecycleService(new ClusterManifestRenderer(), orchestrator))) {
            runtime.start(); HttpClient client = HttpClient.newHttpClient();
            String body = "{\"sql\":\"SELECT SUM(bytes) AS n FROM traffic WHERE ts >= 0 AND ts <= 86399\"}";
            assertThat(post(client, runtime.httpPort(), "/query", body).statusCode()).isEqualTo(200);
            assertThat(post(client, runtime.httpPort(), "/query", body).statusCode()).isEqualTo(200);
            assertThat(executions).hasValue(1);
            var warmed = post(client, runtime.httpPort(), "/cache/warm", "{\"start\":0,\"end\":86399}");
            assertThat(warmed.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(warmed.body()).get("bucketsWarmed").asInt()).isEqualTo(1);
            var size = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + runtime.httpPort() + "/cache/size")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(JSON.readTree(size.body()).get("bucketCount").asInt()).isEqualTo(1);
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", runtime.grpcPort()).usePlaintext().build();
            try {
                var cache = CacheServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                assertThat(cache.measureCache(EmptyRequest.getDefaultInstance()).getBucketCount()).isEqualTo(1);
                assertThat(cache.flushCache(FlushCacheRequest.getDefaultInstance()).getBucketsPurged()).isEqualTo(1);
                var query = QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS).query(QueryRequest.newBuilder()
                        .setSql("SELECT SUM(bytes) AS n FROM traffic WHERE ts >= 0 AND ts <= 86399").build());
                query.forEachRemaining(chunk -> { });
                assertThat(executions).hasValue(3);
                var clusters = ClusterServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
                assertThat(clusters.changeCluster(ClusterRequest.newBuilder().setOperation(ClusterOperation.DEPLOY).build()).getResourcesCount()).isEqualTo(1);
                clusters.changeCluster(ClusterRequest.newBuilder().setOperation(ClusterOperation.STOP).build());
                assertThat(post(client, runtime.httpPort(), "/cluster/start", "{}").statusCode()).isEqualTo(200);
                assertThat(clusterActions).containsExactly("deploy", "scale=0", "scale=1");
                assertThat(cache.flushCache(FlushCacheRequest.getDefaultInstance()).getBucketsPurged()).isZero();
            } finally { channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
            assertThat(post(client, runtime.httpPort(), "/query", "{\"sql\":\"SELECT t.ts FROM traffic t JOIN traffic u ON t.ts=u.ts\"}").statusCode()).isEqualTo(422);
        }
    }
    @Test void rejectsIncompatibleTableClocksBeforeOpeningTheBackend() {
        var application = new ApplicationSettings(ServerSettings.defaults(), WarmingSettings.defaults(),
                List.of(new TableSettings("traffic", "/tmp/traffic", Map.of("ts", "other_clock"), Set.of("ts"))));
        assertThatThrownBy(() -> new CascadaEngineFactory(EngineSettings.localDefaults(), sql -> ResultFrame.empty(), application))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("physical time columns");
    }
}
