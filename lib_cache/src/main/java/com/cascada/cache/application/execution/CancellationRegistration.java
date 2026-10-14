package com.cascada.cache.application.execution;

public record CancellationRegistration(Runnable removeListener) implements AutoCloseable {
    @Override public void close() { removeListener.run(); }
}
