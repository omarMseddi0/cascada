package com.cascada.cache.exception;

/** Discards aggregate work when the source changes while serving the latest data. */
public final class CubeSourceChangedException extends RuntimeException {
    public CubeSourceChangedException() {
        super("Delta source metadata changed during exact cube execution");
    }
}
