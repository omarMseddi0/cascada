package com.cascada.cache.adapter.out.cache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/** A block that awaits pipelined Valkey replies, run under the adapter's shared failure handling. */
@FunctionalInterface
interface ValkeyWait<R> {
    R run() throws InterruptedException, ExecutionException, TimeoutException;
}
