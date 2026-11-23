package com.cascada.cache.application.model.cancellation;

public record CancellationRegistration(Runnable removeListener) implements AutoCloseable {
    @Override public void close() { removeListener.run(); }
}
