package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Everything the catalog keeps for one time window, so evicting or expiring the window drops all of
 * it at once: the shapes and their lattice index, memoised exact answers, the backend revision the
 * answers were computed under, when they expire, and the bytes they retain.
 */
final class CubeWindowShapes {
    private final ShapeLatticeIndex index = new ShapeLatticeIndex();
    private final Set<QueryShape> shapes = new HashSet<>();
    private final Map<QueryShape, ResultFrame> exactAnswers = new HashMap<>();
    private final String revision;
    private Instant expiresAt;
    private long retainedBytes;

    CubeWindowShapes(String revision, Instant expiresAt) {
        this.revision = revision;
        this.expiresAt = expiresAt;
    }

    ShapeLatticeIndex index() { return index; }
    Set<QueryShape> shapes() { return shapes; }
    Map<QueryShape, ResultFrame> exactAnswers() { return exactAnswers; }
    String revision() { return revision; }
    long retainedBytes() { return retainedBytes; }
    void retain(long bytes) { retainedBytes += bytes; }
    boolean expiredAt(Instant now) { return !now.isBefore(expiresAt); }

    /** A source bucket that expires sooner shortens the window's life; it never extends it. */
    void expireNoLaterThan(Instant instant) {
        if (instant.isBefore(expiresAt)) expiresAt = instant;
    }
}
