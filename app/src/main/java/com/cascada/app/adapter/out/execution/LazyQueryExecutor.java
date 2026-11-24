package com.cascada.app.adapter.out.execution;

import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.PinnedQueryExecutorPort;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import java.util.List;
import java.util.Optional;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

/** Starts the execution tier only when a query or warming operation needs it. */
public final class LazyQueryExecutor implements PinnedQueryExecutorPort, AutoCloseable {
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

    @Override
    public Optional<ExactCubeSource> bind(String path, String accessScope) {
        lifecycle.readLock().lock();
        try {
            return initializedExecutor() instanceof PinnedQueryExecutorPort pinned
                    ? pinned.bind(path, accessScope) : Optional.empty();
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public ResultFrame executePinned(ExactCubeSource binding, String physicalSql) {
        lifecycle.readLock().lock();
        try {
            if (initializedExecutor() instanceof PinnedQueryExecutorPort pinned) {
                return pinned.executePinned(binding, physicalSql);
            }
            throw new IllegalStateException("executor does not support pinned Delta reads");
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    @Override
    public ResultFrame executeWithAggregate(ExactCubeSource source, String sql, String relation,
                                            ResultFrame frame, List<String> types) {
        lifecycle.readLock().lock();
        try {
            if (initializedExecutor() instanceof PinnedQueryExecutorPort pinned) {
                return pinned.executeWithAggregate(source, sql, relation, frame, types);
            }
            throw new IllegalStateException("executor does not support typed aggregate relations");
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
