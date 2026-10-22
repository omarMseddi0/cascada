package com.cascada.app.adapter.in.api;

import java.io.*;
import java.nio.file.*;
import org.apache.parquet.io.*;

/** Local export storage uses Java NIO rather than Hadoop's OS-specific filesystem helpers. */
final class NioParquetFile implements InputFile, OutputFile {
    private final Path path;
    NioParquetFile(Path path) { this.path = path; }
    @Override public long getLength() throws IOException { return Files.size(path); }
    @Override public SeekableInputStream newStream() throws IOException {
        FileInputStream input = new FileInputStream(path.toFile());
        return new DelegatingSeekableInputStream(input) {
            @Override public long getPos() throws IOException { return input.getChannel().position(); }
            @Override public void seek(long position) throws IOException { input.getChannel().position(position); }
        };
    }
    @Override public PositionOutputStream create(long blockSizeHint) throws IOException {
        return output(StandardOpenOption.CREATE_NEW);
    }
    @Override public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return output(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }
    private PositionOutputStream output(OpenOption... options) throws IOException {
        OutputStream output = Files.newOutputStream(path, options);
        return new PositionOutputStream() {
            private long position;
            @Override public long getPos() { return position; }
            @Override public void write(int value) throws IOException { output.write(value); position++; }
            @Override public void write(byte[] bytes, int offset, int length) throws IOException {
                output.write(bytes, offset, length); position += length;
            }
            @Override public void flush() throws IOException { output.flush(); }
            @Override public void close() throws IOException { output.close(); }
        };
    }
    @Override public boolean supportsBlockSize() { return false; }
    @Override public long defaultBlockSize() { return 0; }
}
