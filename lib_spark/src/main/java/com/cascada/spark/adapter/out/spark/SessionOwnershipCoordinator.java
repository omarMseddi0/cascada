package com.cascada.spark.adapter.out.spark;

import java.util.function.Supplier;

/** Serializes session acquisition by this adapter and records whether it created the context. */
final class SessionOwnershipCoordinator {

    private final Object lock = new Object();

    <T> SessionLease<T> acquire(Supplier<Boolean> activeContext, Supplier<T> getOrCreate) {
        synchronized (lock) {
            boolean alreadyActive = activeContext.get();
            T session = getOrCreate.get();
            return new SessionLease<>(session, !alreadyActive);
        }
    }
}
