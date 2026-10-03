package com.cascada.cache.adapter.out.serialization;

import com.github.luben.zstd.EndDirective;
import com.github.luben.zstd.ZstdCompressCtx;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;

/** Compresses direct Arrow buffers in bounded chunks, preserving the length-prefixed zstd format. */
final class ArrowZstdChannel implements WritableByteChannel {
    private static final int CHUNK_BYTES = 131_072;
    private final ZstdCompressCtx compressor;
    private final ArrowBuf inputMemory;
    private final ArrowBuf outputMemory;
    private final ByteBuffer input;
    private final ByteBuffer output;
    private final byte[] transfer = new byte[CHUNK_BYTES];
    private final ByteArrayOutputStream blob;
    private final int expectedBytes;
    private long writtenBytes;
    private boolean open = true;

    ArrowZstdChannel(BufferAllocator allocator, int expectedBytes, int level) {
        this.expectedBytes = expectedBytes;
        compressor = new ZstdCompressCtx();
        ArrowBuf allocatedInput = null, allocatedOutput = null;
        try {
            compressor.setLevel(level).setContentSize(true);
            compressor.setPledgedSrcSize(expectedBytes);
            allocatedInput = allocator.buffer(CHUNK_BYTES);
            allocatedOutput = allocator.buffer(CHUNK_BYTES);
            inputMemory = allocatedInput;
            outputMemory = allocatedOutput;
            input = inputMemory.nioBuffer(0, CHUNK_BYTES);
            output = outputMemory.nioBuffer(0, CHUNK_BYTES);
            blob = new ByteArrayOutputStream(Math.max(512, Math.min(1_048_576, expectedBytes / 16)));
            blob.write(expectedBytes >>> 24);
            blob.write(expectedBytes >>> 16);
            blob.write(expectedBytes >>> 8);
            blob.write(expectedBytes);
        } catch (RuntimeException | Error failure) {
            if (allocatedOutput != null) allocatedOutput.close();
            if (allocatedInput != null) allocatedInput.close();
            compressor.close();
            throw failure;
        }
    }

    @Override public int write(ByteBuffer source) throws IOException {
        if (!open) throw new ClosedChannelException();
        int bytes = source.remaining();
        if (source.isDirect()) compress(source);
        else {
            while (source.hasRemaining()) {
                int oldLimit = source.limit();
                source.limit(source.position() + Math.min(source.remaining(), CHUNK_BYTES));
                input.clear();
                input.put(source).flip();
                source.limit(oldLimit);
                compress(input);
            }
        }
        writtenBytes += bytes;
        return bytes;
    }

    private void compress(ByteBuffer source) {
        while (source.hasRemaining()) {
            output.clear();
            compressor.compressDirectByteBufferStream(output, source, EndDirective.CONTINUE);
            drainOutput();
        }
    }
    private void drainOutput() {
        output.flip();
        int bytes = output.remaining();
        output.get(transfer, 0, bytes);
        blob.write(transfer, 0, bytes);
    }
    @Override public boolean isOpen() { return open; }
    @Override public void close() throws IOException {
        if (!open) return;
        open = false;
        try {
            if (writtenBytes != expectedBytes) throw new IOException("Arrow IPC size changed between counting and writing");
            input.clear().limit(0);
            boolean finished;
            do {
                output.clear();
                finished = compressor.compressDirectByteBufferStream(output, input, EndDirective.END);
                drainOutput();
            } while (!finished);
        } finally {
            try { compressor.close(); }
            finally { try { outputMemory.close(); } finally { inputMemory.close(); } }
        }
    }
    byte[] blob() {
        if (open) throw new IllegalStateException("finish the compressed frame before reading its blob");
        return blob.toByteArray();
    }
}
