package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.frame.ResultFrame;

import java.util.List;
import java.util.Optional;

/** Storage operations required by query execution and cache warming. */
public interface BucketCachePort {

    /** Phase 1: existence check; element {@code i} is true iff {@code keys.get(i)} is cached. */
    List<Boolean> existsForKeys(List<String> keys);

    /** Phase 2: bulk fetch; returns the decoded frame for each key, empty where the key was absent. */
    List<Optional<ResultFrame>> multiGet(List<String> keys);

    /** Store (or overwrite) one bucket's frame under its key. */
    void store(String key, ResultFrame frame);
}
