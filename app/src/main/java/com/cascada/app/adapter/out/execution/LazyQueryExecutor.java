package com.cascada.app.adapter.out.execution;

import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** Starts the execution tier only when a query or warming operation needs it. */
public final class LazyQueryExecutor implements QueryExecutorPort, AutoCloseable {
    private final Supplier<? extends QueryExecutorPort> factory;
    private QueryExecutorPort delegate;
    private boolean closed;
    private final ReentrantReadWriteLock lifecycle = new ReentrantReadWriteLock(true);

    public LazyQueryExecutor(Supplier<? extends QueryExecutorPort> factory) {
        this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public ResultFrame execute(String physicalSql) {
        lifecycle.readLock().lock();
        try {
            return initializedExecutor().execute(physicalSql);
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private synchronized QueryExecutorPort initializedExecutor() {
        if (closed) throw new IllegalStateException("query executor is closed");
        if (delegate == null) delegate = Objects.requireNonNull(factory.get(), "executor factory result");
        return delegate;
    }

    @Override
    public void close() throws Exception {
        lifecycle.writeLock().lock();
        try {
            if (closed) return;
            closed = true;
            if (delegate instanceof AutoCloseable closeable) closeable.close();
        } finally {
            lifecycle.writeLock().unlock();
        }
    }
}
