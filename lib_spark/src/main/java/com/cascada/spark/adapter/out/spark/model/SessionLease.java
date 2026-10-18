package com.cascada.spark.adapter.out.spark.model;

/** A session acquired by this adapter, with ownership limited to a context it created. */
public record SessionLease<T>(T session, boolean ownsContext) {
}
