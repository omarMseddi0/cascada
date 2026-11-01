package com.cascada.app.model.api;

import com.cascada.app.exception.InvalidRequestException;
import com.cascada.cache.exception.UncacheableQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.concurrent.*;

public record ApiFailure(int httpStatus, String code, String message) {
    public static ApiFailure from(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) return from(failure.getCause());
        if (failure instanceof InvalidRequestException) return new ApiFailure(400, "INVALID_REQUEST", failure.getMessage());
        if (failure instanceof JsonProcessingException) return new ApiFailure(400, "INVALID_REQUEST", "Malformed JSON request");
        if (failure instanceof UncacheableQueryException) return new ApiFailure(422, "UNSUPPORTED_QUERY", "Query cannot be translated by the configured catalog");
        if (failure instanceof RejectedExecutionException) return new ApiFailure(503, "SERVICE_UNAVAILABLE", "Server is stopping");
        if (failure instanceof CancellationException || failure instanceof InterruptedException) return new ApiFailure(503, "QUERY_CANCELLED", "Query was cancelled");
        return new ApiFailure(500, "INTERNAL_ERROR", "Operation failed");
    }
    public io.grpc.StatusRuntimeException grpcException() {
        io.grpc.Status status = switch (code) {
            case "INVALID_REQUEST" -> io.grpc.Status.INVALID_ARGUMENT;
            case "UNSUPPORTED_QUERY" -> io.grpc.Status.UNIMPLEMENTED;
            case "SERVICE_UNAVAILABLE" -> io.grpc.Status.UNAVAILABLE;
            case "QUERY_CANCELLED" -> io.grpc.Status.CANCELLED;
            default -> io.grpc.Status.INTERNAL;
        };
        return status.withDescription(message).asRuntimeException();
    }
}
