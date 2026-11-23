package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.adapter.in.model.api.ParquetResult;
import com.cascada.app.concurrent.RequestExecutor;
import com.cascada.app.model.config.ServerSettings;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import io.grpc.Status;
import io.grpc.stub.*;
import java.util.concurrent.*;

/** Streams query responses with gRPC readiness and request cancellation handling. */
public final class QueryGrpcService extends QueryServiceGrpc.QueryServiceImplBase implements AutoCloseable {
    private final ExecuteLogicalQueryUseCase queries;
    private final RequestExecutor requests;
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
        var ready = new Semaphore(0);
        stream.setOnReadyHandler(ready::release);
        QueryCancellation cancellation = new QueryCancellation();
        var submitted = new java.util.concurrent.atomic.AtomicReference<Future<?>>();
        Runnable cancel = () -> {
            cancellation.cancel();
            java.util.Optional.ofNullable(submitted.get()).ifPresent(task -> task.cancel(true));
            ready.release();
        };
        stream.setOnCancelHandler(cancel);
        try {
            submitted.set(requests.submit(cancellation, () -> respond(request, stream, ready), () -> { }));
            if (stream.isCancelled()) cancel.run();
        }
        catch (RejectedExecutionException busy) {
            observer.onError(GrpcFailures.exception(busy));
        }
    }

    private void respond(QueryRequest request, ServerCallStreamObserver<QueryChunk> stream, Semaphore ready) {
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
                try (var parquet = new ParquetResult(QueryResponseEncoder.parquet(frame))) {
                    send(stream, ready, metadata.build());
                    try (var output = payloadStream(stream, ready)) {
                        parquet.copyTo(output);
                    }
                }
            } else {
                send(stream, ready, metadata.build());
                if (request.getFormat() == ResponseFormat.JSON) {
                    try (var output = payloadStream(stream, ready)) {
                        QueryResponseEncoder.json(frame, output);
                    }
                } else sendRows(frame, stream, ready);
            }
            if (!stream.isCancelled()) stream.onCompleted();
        } catch (io.grpc.StatusRuntimeException status) {
            if (!stream.isCancelled()) stream.onError(status);
        } catch (Throwable failure) {
            if (!stream.isCancelled()) stream.onError(GrpcFailures.exception(failure));
            if (failure instanceof Error fatal) throw fatal;
        }
    }

    /** JSON and Parquet payloads are cut into about 64 KiB chunks, each sent when the client is ready. */
    private static GrpcChunkOutputStream payloadStream(ServerCallStreamObserver<QueryChunk> stream, Semaphore ready) {
        return new GrpcChunkOutputStream(payload -> send(stream, ready, QueryChunk.newBuilder().setPayload(payload).build()));
    }

    private static void sendRows(ResultFrame frame, ServerCallStreamObserver<QueryChunk> stream, Semaphore ready) {
        var chunk = QueryChunk.newBuilder();
        int chunkBytes = 0;
        ProtobufCellWriter cells = new ProtobufCellWriter();
        FrameColumnReader[] columns = ResultCells.readers(frame);
        for (int rowIndex = 0; rowIndex < frame.rowCount(); rowIndex++) {
            var row = Row.newBuilder();
            if ((rowIndex & 1023) == 0) QueryCancellation.requireCurrentActive();
            for (FrameColumnReader column : columns) {
                cells.startCell();
                ResultCells.visit(column, rowIndex, cells);
                row.addCells(cells.cell());
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
    private static void send(ServerCallStreamObserver<QueryChunk> stream, Semaphore ready, QueryChunk chunk) {
        // Consume old readiness signals before checking stream state. A later callback can wake
        // the request even if it fires between the check and acquire.
        ready.drainPermits();
        while (!stream.isReady() && !stream.isCancelled()) {
            try {
                ready.acquire();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw Status.CANCELLED.asRuntimeException();
            }
        }
        if (stream.isCancelled()) throw Status.CANCELLED.asRuntimeException();
        stream.onNext(chunk);
    }
    @Override public void close() {
        requests.close();
    }
}
