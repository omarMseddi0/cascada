package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.exception.CacheSerializationException;
import java.util.Optional;

/** Result of one worker decode, with corruption handled by the ordered producer. */
final class DecodeOutcome {
    private final Optional<ResultFrame> frame;
    private final CacheSerializationException corruption;

    private DecodeOutcome(Optional<ResultFrame> frame, CacheSerializationException corruption) {
        this.frame = frame;
        this.corruption = corruption;
    }

    static DecodeOutcome missing() {
        return new DecodeOutcome(Optional.empty(), null);
    }

    static DecodeOutcome decoded(ResultFrame frame) {
        return new DecodeOutcome(Optional.of(frame), null);
    }

    static DecodeOutcome corrupt(CacheSerializationException corruption) {
        return new DecodeOutcome(Optional.empty(), corruption);
    }

    Optional<ResultFrame> frame() { return frame; }

    CacheSerializationException corruption() { return corruption; }
}
