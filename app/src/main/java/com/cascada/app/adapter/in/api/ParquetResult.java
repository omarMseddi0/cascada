package com.cascada.app.adapter.in.api;

import java.io.IOException;
import java.nio.file.Path;

record ParquetResult(Path path) implements AutoCloseable {
    @Override public void close() throws IOException { QueryResponseEncoder.deleteParquet(path); }
}
