package com.cascada.cache.exception;

/** Signals that cached cube values cannot safely answer the requested SQL shape. */
public final class CubeRollUpUnavailableException extends RuntimeException {
    public CubeRollUpUnavailableException(String message) {
        super(message);
    }
}
