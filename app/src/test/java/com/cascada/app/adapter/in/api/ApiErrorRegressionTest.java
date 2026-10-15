package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.QueryRequest;
import com.cascada.app.adapter.in.grpc.protocol.QueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiErrorRegressionTest {
    @Test
    void anErrorInTheHttpWorkerReturns500InsteadOfHanging() throws Exception {
        try (var server = new QueryHttpServer(0, sql -> { throw new AssertionError("worker failure"); })) {
            server.start();
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + server.port() + "/query"))
                    .timeout(Duration.ofSeconds(3))
                    .POST(HttpRequest.BodyPublishers.ofString("{\"sql\":\"SELECT n\"}")).build();
            var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body()).contains("INTERNAL_ERROR").doesNotContain("worker failure");
        }
    }

    @Test
    void anErrorInTheGrpcWorkerClosesTheCallWithInternalStatus() throws Exception {
        try (var service = new QueryGrpcService(sql -> { throw new AssertionError("worker failure"); })) {
            Server server = ServerBuilder.forPort(0).addService(service).build().start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            try {
                var stub = QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS);
                assertThatThrownBy(() -> stub.query(QueryRequest.newBuilder().setSql("SELECT n").build()).hasNext())
                        .isInstanceOf(StatusRuntimeException.class)
                        .satisfies(failure -> assertThat(((StatusRuntimeException) failure).getStatus().getCode())
                                .isEqualTo(Status.Code.INTERNAL));
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }
}
