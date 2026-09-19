package com.cascada.cache.domain;

/** The SQL is executable, but cannot be represented safely by the cache planner. */
public class UncacheableQueryException extends RuntimeException {
    public UncacheableQueryException(String message) { super(message); }
    public UncacheableQueryException(String message, Throwable cause) { super(message, cause); }
}
