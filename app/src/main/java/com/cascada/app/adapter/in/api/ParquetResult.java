package com.cascada.app.adapter.in.api;

import java.io.IOException;
import java.nio.file.Path;

record ParquetResult(Path path) implements AutoCloseable {
    long size() throws IOException { return java.nio.file.Files.size(path); }

    /** Streams the finished file; the HTTP body and the gRPC chunk stream both copy it this way. */
    void copyTo(java.io.OutputStream output) throws IOException { java.nio.file.Files.copy(path, output); }

    @Override public void close() throws IOException { QueryResponseEncoder.deleteParquet(path); }
}
