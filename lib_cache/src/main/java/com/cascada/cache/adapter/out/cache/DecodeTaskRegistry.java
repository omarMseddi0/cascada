package com.cascada.cache.adapter.out.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;

/** Tracks only one cache visit's outstanding decode tasks and wakes its producer on cancellation. */
final class DecodeTaskRegistry {
    private final Executor executor;
    private final Set<FutureTask<?>> tasks = new HashSet<>();
    private boolean cancelled;

    DecodeTaskRegistry(Executor executor) {
        this.executor = executor;
    }

    synchronized boolean register(FutureTask<?> task) {
        if (cancelled) {
            task.cancel(true);
            return false;
        }
        tasks.add(task);
        return true;
    }

    synchronized void remove(FutureTask<?> task) {
        tasks.remove(task);
    }

    void cancel() {
        List<FutureTask<?>> pending;
        synchronized (this) {
            cancelled = true;
            pending = new ArrayList<>(tasks);
            tasks.clear();
        }
        for (FutureTask<?> task : pending) {
            try {
                task.cancel(true);
            } catch (RuntimeException ignored) {
                // Preserve the original query cancellation or cache failure.
            }
            if (executor instanceof ThreadPoolExecutor pool) {
                pool.remove(task);
            }
        }
    }
}
