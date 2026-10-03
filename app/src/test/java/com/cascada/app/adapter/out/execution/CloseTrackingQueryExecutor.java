package com.cascada.app.adapter.out.execution;

import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.concurrent.atomic.AtomicInteger;

record CloseTrackingQueryExecutor(AtomicInteger closeCount) implements QueryExecutorPort, AutoCloseable {
    @Override
    public ResultFrame execute(String physicalSql) {
        return ResultFrame.empty();
    }

    @Override
    public void close() {
        closeCount.incrementAndGet();
    }
}
