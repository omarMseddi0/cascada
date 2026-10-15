package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/** Storage operations required by query execution and cache warming. */
public interface BucketCachePort {

    /** Phase 1: existence check; element {@code i} is true iff {@code keys.get(i)} is cached. */
    List<Boolean> existsForKeys(List<String> keys);

    /** Phase 2: bulk fetch; returns the decoded frame for each key, empty where the key was absent. */
    List<Optional<ResultFrame>> multiGet(List<String> keys);

    /** Ordered delivery: index i corresponds to keys[i]. Adapters can decode and release one frame at a time. */
    default void visitKeys(List<String> keys, BiConsumer<Integer, Optional<ResultFrame>> visitor) {
        List<Optional<ResultFrame>> frames = multiGet(keys);
        if (frames == null || frames.size() != keys.size() || frames.stream().anyMatch(java.util.Objects::isNull)) {
            throw new IllegalStateException("cache returned an invalid number of frames or a null frame entry");
        }
        for (int index = 0; index < frames.size(); index++) visitor.accept(index, frames.get(index));
    }

    /** Store (or overwrite) one bucket's frame under its key. */
    void store(String key, ResultFrame frame);
}
