package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.concurrent.RequestExecutor;
import com.cascada.app.config.ServerSettings;
import com.cascada.app.exception.ApiFailure;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.execution.QueryExecutionScope;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.domain.frame.ResultFrame;
import io.grpc.Status;
import io.grpc.stub.*;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;

/** Streaming transport with bounded admission and gRPC readiness/cancellation handling. */
public final class QueryGrpcService extends QueryServiceGrpc.QueryServiceImplBase implements AutoCloseable {
    private final ExecuteLogicalQueryUseCase queries;
    private final RequestExecutor requests;
    private final java.util.Set<QueryCancellation> activeQueries = ConcurrentHashMap.newKeySet();
    public QueryGrpcService(ExecuteLogicalQueryUseCase queries) { this(queries, ServerSettings.defaults()); }
    public QueryGrpcService(ExecuteLogicalQueryUseCase queries, ServerSettings settings) {
        this.queries = queries;
        requests = new RequestExecutor(settings);
    }

    @Override public void query(QueryRequest request, StreamObserver<QueryChunk> observer) {
        if (request.getSql().isBlank() || request.getFormat() == ResponseFormat.UNRECOGNIZED) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription("sql and a supported format are required").asRuntimeException());
            return;
        }
        ServerCallStreamObserver<QueryChunk> stream = (ServerCallStreamObserver<QueryChunk>) observer;
        Object ready = new Object();
        Runnable wake = () -> { synchronized (ready) { ready.notifyAll(); } };
        stream.setOnReadyHandler(wake);
        QueryCancellation cancellation = new QueryCancellation();
        var submitted = new java.util.concurrent.atomic.AtomicReference<Future<?>>();
        Runnable cancel = () -> {
            cancellation.cancel();
            java.util.Optional.ofNullable(submitted.get()).ifPresent(task -> task.cancel(true));
            wake.run();
        };
        stream.setOnCancelHandler(cancel);
        try {
            submitted.set(requests.submit(() -> {
                activeQueries.add(cancellation);
                try (var scope = new QueryExecutionScope(cancellation)) { respond(request, stream, ready); }
                finally { activeQueries.remove(cancellation); }
            }));
            if (stream.isCancelled()) cancel.run();
        }
        catch (RejectedExecutionException busy) {
            observer.onError(Status.UNAVAILABLE.withDescription("Server is stopping").asRuntimeException());
        }
    }

    private void respond(QueryRequest request, ServerCallStreamObserver<QueryChunk> stream, Object ready) {
        try {
            if (stream.isCancelled()) return;
            var result = queries.query(request.getSql());
            if (stream.isCancelled()) return;
            ResultFrame frame = result.frame();
            String contentType = switch (request.getFormat()) {
                case JSON -> "application/json";
                case PARQUET -> "application/vnd.apache.parquet";
                default -> "application/x-protobuf";
            };
            var metadata = QueryChunk.newBuilder().setContentType(contentType)
                    .setRowCount(frame.rowCount()).setServedThroughCache(result.servedThroughCache());
            frame.columnNames().forEach(name -> metadata.addColumns(Column.newBuilder()
                    .setName(name).setType(frame.columnType(name).name())));
            if (request.getFormat() == ResponseFormat.PARQUET) {
                // Complete the footer before sending headers; failed exports return a clean RPC error.
                try (var parquet = new ParquetResult(QueryResponseEncoder.parquet(frame));
                     InputStream input = Files.newInputStream(parquet.path())) {
                    send(stream, ready, metadata.build());
                    while (true) {
                        byte[] ownedBuffer = new byte[64 * 1024];
                        int length = input.read(ownedBuffer);
                        if (length == -1) break;
                        send(stream, ready, QueryChunk.newBuilder().setPayload(
                                com.google.protobuf.UnsafeByteOperations.unsafeWrap(ownedBuffer, 0, length)).build());
                    }
                }
            } else {
                send(stream, ready, metadata.build());
                if (request.getFormat() == ResponseFormat.JSON) {
                    try (var output = new GrpcChunkOutputStream(payload -> send(stream, ready,
                            QueryChunk.newBuilder().setPayload(payload).build()))) {
                        QueryResponseEncoder.json(frame, output);
                    }
                } else sendRows(frame, stream, ready);
            }
            if (!stream.isCancelled()) stream.onCompleted();
        } catch (io.grpc.StatusRuntimeException status) {
            if (!stream.isCancelled()) stream.onError(status);
        } catch (Throwable failure) {
            if (!stream.isCancelled()) stream.onError(ApiFailure.from(failure).grpcException());
            if (failure instanceof Error fatal) throw fatal;
        }
    }

    private static void sendRows(ResultFrame frame, ServerCallStreamObserver<QueryChunk> stream, Object ready) {
        var chunk = QueryChunk.newBuilder();
        int chunkBytes = 0;
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            var row = Row.newBuilder();
            for (int columnIndex = 0; columnIndex < frame.columnNames().size(); columnIndex++) {
                var cell = Cell.newBuilder();
                if (!frame.isNullAt(rowIndex, columnIndex)) {
                    switch (frame.columnTypeAt(columnIndex)) {
                        case LONG -> cell.setLongValue(frame.longAt(rowIndex, columnIndex));
                        case DOUBLE -> cell.setDoubleValue(frame.doubleAt(rowIndex, columnIndex));
                        case STRING -> cell.setStringValue(frame.stringAt(rowIndex, columnIndex));
                        case DECIMAL -> cell.setDecimalValue(frame.valueAt(rowIndex, columnIndex).toString());
                    }
                }
                row.addCells(cell);
            }
            var encoded = row.build();
            int encodedBytes = encoded.getSerializedSize() + 5;
            if (chunk.getRowsCount() > 0 && chunkBytes + encodedBytes > 64 * 1024) {
                send(stream, ready, chunk.build()); chunk.clear(); chunkBytes = 0;
            }
            chunk.addRows(encoded); chunkBytes += encodedBytes;
        }
        if (chunk.getRowsCount() > 0) send(stream, ready, chunk.build());
    }
    private static void send(ServerCallStreamObserver<QueryChunk> stream, Object ready, QueryChunk chunk) {
        synchronized (ready) {
            while (!stream.isReady() && !stream.isCancelled()) {
                try { ready.wait(1000); }
                catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw Status.CANCELLED.asRuntimeException();
                }
            }
        }
        if (stream.isCancelled()) throw Status.CANCELLED.asRuntimeException();
        stream.onNext(chunk);
    }
    @Override public void close() {
        activeQueries.forEach(QueryCancellation::cancel);
        requests.close();
    }
}
