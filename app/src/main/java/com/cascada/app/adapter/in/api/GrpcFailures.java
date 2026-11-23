package com.cascada.app.adapter.in.api;

import com.cascada.app.adapter.in.model.api.ApiFailure;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

final class GrpcFailures {
    private GrpcFailures() {
    }

    static Status status(Throwable failure) {
        ApiFailure error = ApiFailure.from(failure);
        Status status = switch (error.code()) {
            case "INVALID_REQUEST" -> Status.INVALID_ARGUMENT;
            case "UNSUPPORTED_QUERY" -> Status.UNIMPLEMENTED;
            case "SERVICE_UNAVAILABLE" -> Status.UNAVAILABLE;
            case "QUERY_CANCELLED" -> Status.CANCELLED;
            default -> Status.INTERNAL;
        };
        return status.withDescription(error.message());
    }

    static StatusRuntimeException exception(Throwable failure) {
        return status(failure).asRuntimeException();
    }
}
