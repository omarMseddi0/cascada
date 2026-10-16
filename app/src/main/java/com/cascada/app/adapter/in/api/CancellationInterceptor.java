package com.cascada.app.adapter.in.api;

import com.cascada.app.concurrent.RequestExecutor;
import com.cascada.app.config.ServerSettings;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.app.exception.ApiFailure;
import io.grpc.*;
import java.util.concurrent.*;

/** Bounded execution and request-scoped cancellation for the existing unary administration ports. */
public final class CancellationInterceptor implements ServerInterceptor, AutoCloseable {
    private final RequestExecutor requests;
    public CancellationInterceptor(ServerSettings settings) {
        requests = new RequestExecutor(settings);
    }
    @Override public <Request, Response> ServerCall.Listener<Request> interceptCall(
            ServerCall<Request, Response> call, Metadata headers, ServerCallHandler<Request, Response> next) {
        QueryCancellation cancellation = new QueryCancellation();
        Context context = Context.current();
        ServerCall.Listener<Request> delegate = next.startCall(call, headers);
        Runnable operation = context.wrap(() -> {
            try { delegate.onHalfClose(); }
            catch (Throwable failure) {
                call.close(ApiFailure.from(failure).grpcException().getStatus(), new Metadata());
                if (failure instanceof Error fatal) throw fatal;
            }
        });
        Runnable cancelledBeforeStart = () -> call.close(Status.CANCELLED, new Metadata());
        var submitted = new java.util.concurrent.atomic.AtomicReference<Future<?>>();
        Context.CancellationListener listener = ignored -> {
            cancellation.cancel(); java.util.Optional.ofNullable(submitted.get()).ifPresent(task -> task.cancel(true));
        };
        context.addListener(listener, Runnable::run);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
            @Override public void onHalfClose() {
                try {
                    submitted.set(requests.submit(cancellation, operation, cancelledBeforeStart));
                    if (context.isCancelled()) listener.cancelled(context);
                }
                catch (RejectedExecutionException busy) {
                    context.removeListener(listener);
                    call.close(ApiFailure.from(busy).grpcException().getStatus(), new Metadata());
                }
            }
            @Override public void onComplete() { context.removeListener(listener); super.onComplete(); }
            @Override public void onCancel() { listener.cancelled(context); context.removeListener(listener); super.onCancel(); }
        };
    }
    @Override public void close() {
        requests.close();
    }
}
