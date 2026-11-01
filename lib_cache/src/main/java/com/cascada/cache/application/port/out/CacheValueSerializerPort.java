package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CacheSerializationException;

/**
 * Serializes a {@link ResultFrame} to a compressed blob and back, porting the two-stage
 * (serialize → compress level 9) contract of {@code cache_serialization.py}. The production adapter
 * uses Apache Arrow IPC + zstd (language-neutral, columnar — HARNESS §B.5.5); a portable adapter
 * provides the same contract without Arrow for environments that cannot open the required JDK module.
 *
 * <p>Corruption is surfaced as {@link CacheSerializationException}, mirroring the Python
 * {@code RuntimeError} on a failed decompress/deserialize.
 */
public interface CacheValueSerializerPort {

    byte[] serialize(ResultFrame frame);

    ResultFrame deserialize(byte[] blob);


}
