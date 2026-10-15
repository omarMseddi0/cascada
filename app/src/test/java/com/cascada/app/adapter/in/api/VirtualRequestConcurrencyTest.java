package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.config.ServerSettings;
import com.cascada.cache.application.result.QueryResult;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class VirtualRequestConcurrencyTest {
    @Test void fiftyTwoHttpQueriesEachUseTheirOwnVirtualThread() throws Exception {
        CountDownLatch entered = new CountDownLatch(52), release = new CountDownLatch(1);
        AtomicInteger virtualRequests = new AtomicInteger();
        var settings = new ServerSettings(0, 0, Duration.ofSeconds(2));
        var shared = ResultFrame.builder().column("n", ColumnType.LONG).row(7L).build();
        try (var server = new QueryHttpServer(settings, sql -> {
            if (Thread.currentThread().isVirtual()) virtualRequests.incrementAndGet();
            entered.countDown();
            try { release.await(10, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return new QueryResult(shared, true);
        })) {
            server.start(); HttpClient client = HttpClient.newHttpClient();
            var requests = java.util.stream.IntStream.range(0, 52).mapToObj(index -> client.sendAsync(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/query"))
                            .timeout(Duration.ofSeconds(15)).POST(HttpRequest.BodyPublishers.ofString("{\"sql\":\"SELECT n\"}"))
                            .build(), HttpResponse.BodyHandlers.ofString())).toList();
            assertThat(entered.await(8, TimeUnit.SECONDS)).isTrue();
            assertThat(virtualRequests).hasValue(52);
            release.countDown();
            for (var request : requests) {
                var response = request.get(10, TimeUnit.SECONDS);
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(new ObjectMapper().readTree(response.body()).get(0).get("n").longValue()).isEqualTo(7);
            }
        } finally { release.countDown(); }
    }
    @Test void fiftyTwoGrpcQueriesKeepIndependentResponseStreams() throws Exception {
        CountDownLatch entered = new CountDownLatch(52), release = new CountDownLatch(1);
        AtomicInteger virtualRequests = new AtomicInteger();
        var settings = new ServerSettings(0, 0, Duration.ofSeconds(2));
        try (var service = new QueryGrpcService(sql -> {
            if (Thread.currentThread().isVirtual()) virtualRequests.incrementAndGet();
            entered.countDown();
            try { release.await(10, TimeUnit.SECONDS); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return new QueryResult(ResultFrame.builder().column("n", ColumnType.LONG)
                    .row(Long.parseLong(sql)).build(), false);
        }, settings)) {
            Server server = ServerBuilder.forPort(0).addService(service).build().start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
            try {
                var requests = java.util.stream.IntStream.range(0, 52).mapToObj(index -> clients.submit(() -> {
                    var response = QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(15, TimeUnit.SECONDS)
                            .query(QueryRequest.newBuilder().setSql(Integer.toString(index)).build());
                    assertThat(response.next().getRowCount()).isEqualTo(1);
                    long value = response.next().getRows(0).getCells(0).getLongValue();
                    assertThat(response.hasNext()).isFalse(); return value;
                })).toList();
                assertThat(entered.await(8, TimeUnit.SECONDS)).isTrue();
                assertThat(virtualRequests).hasValue(52); release.countDown();
                for (int index = 0; index < requests.size(); index++) assertThat(requests.get(index).get(10, TimeUnit.SECONDS)).isEqualTo((long) index);
            } finally {
                release.countDown(); clients.shutdownNow();
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally { release.countDown(); }
    }
    @Test void publishedGrpcBuffersRemainImmutableAfterSubsequentWrites() throws Exception {
        java.util.List<com.google.protobuf.ByteString> chunks = new java.util.ArrayList<>();
        try (var output = new GrpcChunkOutputStream(chunks::add)) {
            byte[] first = new byte[65536]; java.util.Arrays.fill(first, (byte) 1);
            output.write(first);
            byte[] second = new byte[65536]; java.util.Arrays.fill(second, (byte) 2);
            output.write(second);
        }
        assertThat(chunks).hasSize(2);
        assertThat(chunks.get(0).byteAt(0)).isEqualTo((byte) 1);
        assertThat(chunks.get(0).byteAt(65535)).isEqualTo((byte) 1);
        assertThat(chunks.get(1).byteAt(0)).isEqualTo((byte) 2);
    }
}
