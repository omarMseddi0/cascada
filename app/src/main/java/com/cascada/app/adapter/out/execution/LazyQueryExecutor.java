package com.cascada.app.adapter.out.execution;

import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/** Starts the execution tier only when a query or warming operation needs it. */
public final class LazyQueryExecutor implements QueryExecutorPort, AutoCloseable {
    private final Supplier<? extends QueryExecutorPort> factory;
    private QueryExecutorPort delegate;
    private final java.util.concurrent.locks.ReentrantLock initialization = new java.util.concurrent.locks.ReentrantLock();
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

    private QueryExecutorPort initializedExecutor() {
        initialization.lock();
        try {
            if (closed) throw new IllegalStateException("Query executor is closed");
            if (delegate == null) delegate = Objects.requireNonNull(factory.get(), "executor factory result");
            return delegate;
        } finally { initialization.unlock(); }
    }

    @Override
    public void close() throws Exception {
        if (!lifecycle.writeLock().tryLock(30, java.util.concurrent.TimeUnit.SECONDS))
            throw new IllegalStateException("Active query execution did not finish within the shutdown timeout");
        try {
            if (closed) return;
            closed = true;
            if (delegate instanceof AutoCloseable closeable) closeable.close();
        } finally {
            lifecycle.writeLock().unlock();
        }
    }
}
