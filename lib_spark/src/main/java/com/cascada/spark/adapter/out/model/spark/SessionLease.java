package com.cascada.spark.adapter.out.model.spark;

/** A session acquired by this adapter, with ownership limited to a context it created. */
public record SessionLease<T>(T session, boolean ownsContext) {
}
