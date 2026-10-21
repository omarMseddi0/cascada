package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.model.config.ServerSettings;
import com.cascada.app.exception.ConfigurationException;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.model.QueryResult;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.sql.exception.UnsupportedSqlException;
import io.grpc.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueryBoundaryRegressionTest {
    private static QueryResult result() {
        return new QueryResult(ResultFrame.builder().column("n", ColumnType.LONG).row(1L).build(), false);
    }
    private static HttpRequest request(int port, String path, String body) {
        return HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
    }
    @Test void infrastructureAndUnsupportedQueriesHaveDifferentStatuses() throws Exception {
        var client = HttpClient.newHttpClient();
        try (var server = new QueryHttpServer(0, sql -> { throw new ConfigurationException("private file path", new java.io.IOException()); })) {
            server.start();
            var response = client.send(request(server.port(), "/query", "{\"sql\":\"SELECT n\"}"), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body()).contains("INTERNAL_ERROR").doesNotContain("private file path");
        }
        try (var server = new QueryHttpServer(0, sql -> { throw new UnsupportedSqlException("JOIN"); })) {
            server.start();
            assertThat(client.send(request(server.port(), "/query", "{\"sql\":\"SELECT n\"}"), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(422);
            assertThat(client.send(request(server.port(), "/query", "{\"format\":\"json\"}"), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(400);
        }
    }
    @Test void concurrentQueriesKeepHealthResponsiveWithoutAnApplicationGate() throws Exception {
        CountDownLatch started = new CountDownLatch(1), release = new CountDownLatch(1);
        AtomicReference<String> executionThread = new AtomicReference<>();
        var settings = new ServerSettings(0, 0, Duration.ofSeconds(2));
        var client = HttpClient.newHttpClient();
        try (var server = new QueryHttpServer(settings, sql -> {
            executionThread.set(Thread.currentThread().getName()); started.countDown();
            try { release.await(5, TimeUnit.SECONDS); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return result();
        })) {
            server.start();
            var first = client.sendAsync(request(server.port(), "/query", "{\"sql\":\"SELECT n\"}"), HttpResponse.BodyHandlers.ofString());
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            var pending = java.util.stream.IntStream.range(0, 8).mapToObj(index -> client.sendAsync(
                    request(server.port(), "/query", "{\"sql\":\"SELECT n\"}"), HttpResponse.BodyHandlers.ofString())).toList();
            var health = client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/health"))
                    .timeout(Duration.ofSeconds(2)).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(executionThread.get()).doesNotContain("HTTP-Dispatcher");
            release.countDown(); first.get(3, TimeUnit.SECONDS);
            for (var response : pending) assertThat(response.get(3, TimeUnit.SECONDS).statusCode()).isEqualTo(200);
        } finally { release.countDown(); }
    }
    @Test void grpcDeadlineCancelsExecutionAndReleasesItsWorker() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1), cancelled = new CountDownLatch(1), completed = new CountDownLatch(1);
        var settings = new ServerSettings(0, 0, Duration.ofSeconds(2));
        try (var service = new QueryGrpcService(sql -> {
            if (calls.incrementAndGet() == 1) {
                try (var listener = QueryExecutionScope.current().orElseThrow().onCancel(cancelled::countDown)) {
                    entered.countDown();
                    try { cancelled.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                    QueryExecutionScope.current().orElseThrow().requireActive();
                } finally { completed.countDown(); }
            }
            return result();
        }, settings)) {
            Server server = ServerBuilder.forPort(0).addService(service).build().start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            try {
                var stub = QueryServiceGrpc.newBlockingStub(channel);
                assertThatThrownBy(() -> stub.withDeadlineAfter(1, TimeUnit.SECONDS).query(
                        QueryRequest.newBuilder().setSql("SELECT n").build()).hasNext()).isInstanceOf(StatusRuntimeException.class);
                assertThat(entered.getCount()).isZero();
                assertThat(cancelled.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(completed.await(2, TimeUnit.SECONDS)).isTrue();
                var response = stub.withDeadlineAfter(2, TimeUnit.SECONDS).query(QueryRequest.newBuilder().setSql("SELECT n").build());
                assertThat(response.next().getRowCount()).isEqualTo(1);
                response.forEachRemaining(chunk -> { });
            } finally { channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS); }
        }
    }
}
