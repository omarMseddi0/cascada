package com.cascada.cache.application.service.cube;

import com.cascada.cache.domain.model.cube.CachedShapeEntry;
import com.cascada.cache.domain.model.cube.QueryShape;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.domain.model.time.TimeRange;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The in-memory cube catalog that connects the subsumption algebra to the execution engine
 * (plan §8.12, Appendix I): completed full-window answers are registered here by shape, and a later
 * <em>finer</em> query over the <strong>exact same time window</strong> can be answered by
 * roll-up/filter-down without touching Spark.
 *
 * <p>Correctness boundaries (general-engine principle: bypass when unsure):
 * <ul>
 *   <li><b>Exact time-range match only.</b> A {@link QueryShape} carries no time bounds, so entries
 *       are keyed by their {@link TimeRange}; a query with a different window never sees them.</li>
 *   <li><b>Verified before served.</b> Every roll-up is re-derived by the independent
 *       {@link CubeConsistencyVerifier} oracle and compared cell-by-cell; any disagreement means the
 *       caller falls through to the normal cache/Spark path.</li>
 *   <li><b>Bounded.</b> At most {@code maxWindows} distinct time windows are kept,
 *       least-recently-used out — the catalog is an accelerator, never an unbounded memory sink.</li>
 * </ul>
 */
public final class CubeShapeCatalog {

    /** Distinct time windows retained by default; dashboards revisit few windows, LRU fits. */
    public static final int DEFAULT_MAX_WINDOWS = 256;
    public static final int DEFAULT_MAX_SHAPES_PER_WINDOW = 64;

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();
    private final CubeConsistencyVerifier verifier = new CubeConsistencyVerifier();
    private static final long MAX_RETAINED_BYTES = 64L * 1024 * 1024;
    private final java.time.Clock clock;
    private final java.time.Duration timeToLive;
    private final int maxWindows;
    private final int maxShapesPerWindow;
    private final Map<TimeRange, CubeWindowShapes> windows;
    private long clearGeneration;

    public CubeShapeCatalog() {
        this(DEFAULT_MAX_WINDOWS, DEFAULT_MAX_SHAPES_PER_WINDOW);
    }

    public CubeShapeCatalog(int maxWindows) {
        this(maxWindows, DEFAULT_MAX_SHAPES_PER_WINDOW);
    }

    public CubeShapeCatalog(int maxWindows, int maxShapesPerWindow) {
        this(maxWindows, maxShapesPerWindow, java.time.Duration.ofMinutes(5), java.time.Clock.systemUTC());
    }

    public CubeShapeCatalog(int maxWindows, int maxShapesPerWindow, java.time.Duration timeToLive, java.time.Clock clock) {
        if (timeToLive == null || timeToLive.isZero() || timeToLive.isNegative())
            throw new IllegalArgumentException("cube TTL must be positive");
        this.clock = java.util.Objects.requireNonNull(clock);
        this.timeToLive = timeToLive;
        if (maxWindows <= 0 || maxShapesPerWindow <= 0) {
            throw new IllegalArgumentException("cube bounds must be positive");
        }
        this.maxWindows = maxWindows;
        this.maxShapesPerWindow = maxShapesPerWindow;
        this.windows = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<TimeRange, CubeWindowShapes> eldest) {
                return size() > CubeShapeCatalog.this.maxWindows;
            }
        };
    }

    /** Shape view of a canonical query's hash DNA, for catalog lookup and registration. */
    public static QueryShape shapeOf(HashComponents components) {
        return new QueryShape(new HashSet<>(components.groupBy()), new HashSet<>(components.filters()),
                new HashSet<>(components.aggregates()));
    }

    /** The production shape includes source and output aliases, both required for safe reuse. */
    public static QueryShape shapeOf(CanonicalQueryObject query) {
        HashComponents components = query.hashComponents();
        return new QueryShape(new HashSet<>(components.groupBy()), new HashSet<>(components.filters()),
                new HashSet<>(components.aggregates()), new HashSet<>(query.sourceSignature()),
                query.metadata().measureAggregates(), query.projectionSignature());
    }

    /**
     * Catalog a complete answer for {@code timeRange} computed at {@code shape}. Empty frames carry
     * no roll-up information; re-registering an already-catalogued shape for the same window is a
     * no-op (the lattice index treats dedup as the caller's concern).
     */
    public void register(TimeRange timeRange, QueryShape shape, ResultFrame frame) {
        register(timeRange, shape, frame, "standalone", java.util.Optional.empty());
    }

    public void register(TimeRange timeRange, QueryShape shape, ResultFrame frame,
                         String revision, java.util.Optional<java.time.Duration> dependencyTtl) {
        if (frame.isEmpty()) return;

        long registrationGeneration;
        java.time.Instant registrationStarted;
        CubeWindowShapes observedWindow;
        synchronized (this) {
            registrationStarted = clock.instant();
            expire(registrationStarted);
            CubeWindowShapes window = windows.get(timeRange);
            observedWindow = window;
            if (hasShape(window, shape, revision)) return;
            registrationGeneration = clearGeneration;
        }

        // Frames are immutable, so the full content scan can run without blocking lookups,
        // invalidation, or registrations for unrelated windows.
        long bytes = estimateBytes(frame);
        synchronized (this) {
            java.time.Instant now = clock.instant();
            expire(now);
            if (clearGeneration != registrationGeneration) return;
            CubeWindowShapes window = windows.get(timeRange);
            // An older scan must not overwrite a different revision published while it was unlocked.
            if (window != observedWindow && window != null && !window.revision().equals(revision)) return;
            if (hasShape(window, shape, revision)) return;
            // Estimation time cannot extend source lifetimes or the catalog's original TTL.
            if (!registrationStarted.plus(timeToLive).isAfter(now)
                    || dependencyTtl.filter(ttl -> !registrationStarted.plus(ttl).isAfter(now)).isPresent()) return;
            if (bytes > MAX_RETAINED_BYTES) return;

            if (window != null && !window.revision().equals(revision)) {
                // Answers computed under an older backend revision may describe data that has since changed.
                windows.remove(timeRange);
                window = null;
            }
            while (!windows.isEmpty() && retainedBytes() + bytes > MAX_RETAINED_BYTES) {
                windows.remove(windows.keySet().iterator().next());
            }
            if (window == null || !windows.containsKey(timeRange)) {
                window = new CubeWindowShapes(revision, registrationStarted.plus(timeToLive));
                windows.put(timeRange, window);
            }
            CubeWindowShapes target = window;
            dependencyTtl.ifPresent(ttl -> target.expireNoLaterThan(registrationStarted.plus(ttl)));
            if (target.shapes().size() < maxShapesPerWindow && target.shapes().add(shape)) {
                target.index().register(new CachedShapeEntry(shape, frame));
                target.retain(bytes);
            }
        }
    }

    private boolean hasShape(CubeWindowShapes window, QueryShape shape, String revision) {
        return window != null && window.revision().equals(revision) && window.shapes().contains(shape);
    }

    /**
     * Answer {@code query} from a registered subsuming shape over the same time window, or empty when
     * no candidate subsumes it or the verifier refuses the roll-up.
     */
    public Optional<ResultFrame> tryAnswer(TimeRange timeRange, QueryShape query) {
        return tryAnswer(timeRange, query, "standalone");
    }

    public Optional<ResultFrame> tryAnswer(TimeRange timeRange, QueryShape query, String revision) {
        CubeWindowShapes window;
        java.util.List<CachedShapeEntry> candidates;
        synchronized (this) {
            expire(clock.instant());
            window = windows.get(timeRange);
            if (window == null || !window.revision().equals(revision)) return Optional.empty();
            ResultFrame exactAnswer = window.exactAnswers().get(query);
            if (exactAnswer != null) return Optional.of(exactAnswer);
            candidates = window.index().candidatesFor(query);
        }
        // Frames are immutable. Expensive regrouping and verification never hold the catalog lock.
        for (CachedShapeEntry candidate : candidates) {
            try {
                ResultFrame answer;
                boolean exact = candidate.shape().equals(query);
                if (exact) answer = candidate.frame();
                else {
                    if (!planner.subsumes(candidate.shape(), query)) continue;
                    answer = planner.rollUpAndFilterDown(candidate, query);
                }
                if (!verifier.verifyRollUp(candidate, query, answer).isConsistent()) continue;
                synchronized (this) {
                    expire(clock.instant());
                    if (windows.get(timeRange) != window) return Optional.empty();
                    if (exact) window.exactAnswers().put(query, answer);
                }
                return Optional.of(answer);
            } catch (RuntimeException unverifiableCandidate) {
                // An unsupported candidate is a miss; Spark/the ordinary bucket path remains authoritative.
            }
        }
        return Optional.empty();
    }

    /** Drop every catalogued window — the flush hook for invalidation / admin cache-flush (§8.17). */
    public synchronized void clear() {
        windows.clear();
        clearGeneration++;
    }

    /** Conservative admission accounting, stopping as soon as the budget is exceeded. */
    private long estimateBytes(ResultFrame frame) {
        long bytes = 256L + (long) frame.rowCount() * frame.columnNames().size() * 32;
        if (bytes > MAX_RETAINED_BYTES) return bytes;
        for (int column = 0; column < frame.columnNames().size(); column++) {
            if (frame.columnTypeAt(column) != com.cascada.cache.domain.frame.ColumnType.STRING
                    && frame.columnTypeAt(column) != com.cascada.cache.domain.frame.ColumnType.DECIMAL) continue;
            for (int row = 0; row < frame.rowCount(); row++) {
                if (frame.isNullAt(row, column)) continue;
                if (frame.columnTypeAt(column) == com.cascada.cache.domain.frame.ColumnType.STRING)
                    bytes += 48L + 2L * frame.stringAt(row, column).length();
                else bytes += 64L + 4L * ((java.math.BigDecimal) frame.valueAt(row, column)).precision();
                if (bytes > MAX_RETAINED_BYTES) return bytes;
            }
        }
        return bytes;
    }

    private void expire(java.time.Instant now) {
        windows.values().removeIf(window -> window.expiredAt(now));
    }

    private long retainedBytes() {
        long total = 0L;
        for (CubeWindowShapes window : windows.values()) total += window.retainedBytes();
        return total;
    }

    public synchronized int windowCount() {
        expire(clock.instant());
        return windows.size();
    }

    public synchronized int entryCount() {
        expire(clock.instant());
        int total = 0;
        for (CubeWindowShapes window : windows.values()) {
            total += window.shapes().size();
        }
        return total;
    }

}
