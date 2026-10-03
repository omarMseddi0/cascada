package com.cascada.spark.adapter.out.spark;

/** A session acquired by this adapter, with ownership limited to a context it created. */
record SessionLease<T>(T session, boolean ownsContext) {
}
