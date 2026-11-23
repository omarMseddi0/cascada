package com.cascada.app.adapter.in.model.api;

import com.cascada.app.exception.InvalidRequestException;
import com.cascada.cache.exception.UncacheableQueryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;

/** Shared failure classification for the HTTP and gRPC adapters. */
public record ApiFailure(String code, String message) {
    public static ApiFailure from(Throwable failure) {
        if (failure instanceof CompletionException && failure.getCause() != null) {
            return from(failure.getCause());
        }
        if (failure instanceof InvalidRequestException) {
            return new ApiFailure("INVALID_REQUEST", failure.getMessage());
        }
        if (failure instanceof JsonProcessingException) {
            return new ApiFailure("INVALID_REQUEST", "Malformed JSON request");
        }
        if (failure instanceof UncacheableQueryException) {
            return new ApiFailure("UNSUPPORTED_QUERY", "Query cannot be translated by the configured catalog");
        }
        if (failure instanceof RejectedExecutionException) {
            return new ApiFailure("SERVICE_UNAVAILABLE", "Server is stopping");
        }
        if (failure instanceof CancellationException || failure instanceof InterruptedException) {
            return new ApiFailure("QUERY_CANCELLED", "Query was cancelled");
        }
        return new ApiFailure("INTERNAL_ERROR", "Operation failed");
    }
}
