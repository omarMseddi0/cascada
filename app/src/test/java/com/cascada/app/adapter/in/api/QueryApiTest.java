package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.cache.application.result.QueryResult;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.*;
import java.io.*;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class QueryApiTest {
    private static ResultFrame frame() {
        return ResultFrame.builder().column("id", ColumnType.LONG).column("score", ColumnType.DOUBLE)
                .column("name", ColumnType.STRING).column("amount", ColumnType.DECIMAL)
                .row(9007199254740993L, 1.25, "hello 🌍", new BigDecimal("123.450"))
                .row(null, null, null, null).build();
    }
    @Test void parquetPreservesTypesNullsAndDecimalPrecision() throws Exception {
        Path file = QueryResponseEncoder.parquet(frame());
        try (var reader = new ParquetReader.Builder<org.apache.parquet.example.data.Group>(new NioParquetFile(file)) {
            @Override protected org.apache.parquet.hadoop.api.ReadSupport<org.apache.parquet.example.data.Group> getReadSupport() {
                return new GroupReadSupport();
            }
        }
                .withConf(new Configuration()).build()) {
            var first = reader.read();
            assertThat(first.getLong("id", 0)).isEqualTo(9007199254740993L);
            assertThat(first.getDouble("score", 0)).isEqualTo(1.25);
            assertThat(first.getString("name", 0)).isEqualTo("hello 🌍");
            assertThat(new BigDecimal(new java.math.BigInteger(first.getBinary("amount", 0).getBytes()), 3))
                    .isEqualByComparingTo("123.450");
            assertThat(reader.read().getFieldRepetitionCount("id")).isZero();
            assertThat(reader.read()).isNull();
        } finally { QueryResponseEncoder.deleteParquet(file); }
        assertThat(file).doesNotExist();
    }
    @Test void httpReturnsJsonParquetAndValidationErrors() throws Exception {
        try (var server = new QueryHttpServer(0, sql -> new QueryResult(frame(), true))) {
            server.start();
            HttpClient client = HttpClient.newHttpClient();
            var uri = URI.create("http://localhost:" + server.port() + "/query");
            var response = client.send(HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.ofString(
                    "{\"sql\":\"SELECT id\",\"format\":\"json\"}")).build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(response.statusCode()).isEqualTo(200);
            var json = new ObjectMapper().readTree(response.body());
            assertThat(json.get(0).get("id").longValue()).isEqualTo(9007199254740993L);
            assertThat(json.get(1).get("name").isNull()).isTrue();
            assertThat(response.headers().firstValue("X-Cascada-Cache")).contains("true");
            var parquet = client.send(HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.ofString(
                    "{\"sql\":\"SELECT id\",\"format\":\"parquet\"}")).build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(parquet.statusCode()).isEqualTo(200);
            assertThat(new String(parquet.body(), 0, 4, java.nio.charset.StandardCharsets.US_ASCII)).isEqualTo("PAR1");
            var invalid = client.send(HttpRequest.newBuilder(uri).POST(HttpRequest.BodyPublishers.ofString(
                    "{\"sql\":\"SELECT id\",\"format\":\"csv\"}")).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(invalid.statusCode()).isEqualTo(400);
        }
    }
    @Test void grpcStreamsTypedRowsAndBothEncodedFormats() throws Exception {
        try (var service = new QueryGrpcService(sql -> new QueryResult(frame(), false))) {
            Server server = ServerBuilder.forPort(0).addService(service).build().start();
            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
            try {
                var stub = QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);
                var chunks = stub.query(QueryRequest.newBuilder().setSql("SELECT id").build());
                var metadata = chunks.next();
                assertThat(metadata.getColumnsCount()).isEqualTo(4);
                assertThat(metadata.getRowCount()).isEqualTo(2);
                assertThat(metadata.getServedThroughCache()).isFalse();
                var rows = chunks.next().getRowsList();
                assertThat(rows.get(0).getCells(0).getLongValue()).isEqualTo(9007199254740993L);
                assertThat(rows.get(0).getCells(3).getDecimalValue()).isEqualTo("123.450");
                assertThat(rows.get(1).getCells(0).getValueCase()).isEqualTo(Cell.ValueCase.VALUE_NOT_SET);
                assertThat(chunks.hasNext()).isFalse();
                for (var format : new ResponseFormat[]{ResponseFormat.JSON, ResponseFormat.PARQUET}) {
                    var encoded = stub.query(QueryRequest.newBuilder().setSql("SELECT id").setFormat(format).build());
                    encoded.next();
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    encoded.forEachRemaining(chunk -> { try { chunk.getPayload().writeTo(body); } catch (IOException e) { throw new UncheckedIOException(e); } });
                    if (format == ResponseFormat.JSON) assertThat(new ObjectMapper().readTree(body.toByteArray()).size()).isEqualTo(2);
                    else assertThat(body.toByteArray()).startsWith((byte)'P', (byte)'A', (byte)'R', (byte)'1');
                }
                assertThatThrownBy(() -> stub.query(QueryRequest.getDefaultInstance()).hasNext())
                        .isInstanceOf(StatusRuntimeException.class)
                        .satisfies(error -> assertThat(((StatusRuntimeException)error).getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
            } finally {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }
}
