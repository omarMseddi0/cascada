package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.TimeRange;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();
    private final CubeConsistencyVerifier verifier = new CubeConsistencyVerifier();
    private final int maxWindows;
    private final Map<TimeRange, WindowShapes> windows;

    public CubeShapeCatalog() {
        this(DEFAULT_MAX_WINDOWS);
    }

    public CubeShapeCatalog(int maxWindows) {
        if (maxWindows <= 0) {
            throw new IllegalArgumentException("maxWindows must be > 0, but was: " + maxWindows);
        }
        this.maxWindows = maxWindows;
        this.windows = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<TimeRange, WindowShapes> eldest) {
                return size() > CubeShapeCatalog.this.maxWindows;
            }
        };
    }

    /** Shape view of a canonical query's hash DNA, for catalog lookup and registration. */
    public static QueryShape shapeOf(HashComponents components) {
        return new QueryShape(new HashSet<>(components.groupBy()), new HashSet<>(components.filters()),
                new HashSet<>(components.aggregates()));
    }

    /**
     * Catalog a complete answer for {@code timeRange} computed at {@code shape}. Empty frames carry
     * no roll-up information; re-registering an already-catalogued shape for the same window is a
     * no-op (the lattice index treats dedup as the caller's concern).
     */
    public synchronized void register(TimeRange timeRange, QueryShape shape, ResultFrame frame) {
        if (frame.isEmpty()) {
            return;
        }
        WindowShapes window = windows.computeIfAbsent(timeRange, ignored -> new WindowShapes());
        if (window.shapes.add(shape)) {
            window.index.register(new CachedShapeEntry(shape, frame));
        }
    }

    /**
     * Answer {@code query} from a registered subsuming shape over the same time window, or empty when
     * no candidate subsumes it or the verifier refuses the roll-up.
     */
    public synchronized Optional<ResultFrame> tryAnswer(TimeRange timeRange, QueryShape query) {
        WindowShapes window = windows.get(timeRange);
        if (window == null) {
            return Optional.empty();
        }
        Optional<CachedShapeEntry> best = window.index.findBestSubsumer(query, planner);
        if (best.isEmpty()) {
            return Optional.empty();
        }
        ResultFrame answer = planner.rollUpAndFilterDown(best.get(), query);
        if (!verifier.verifyRollUp(best.get(), query, answer).isConsistent()) {
            return Optional.empty();
        }
        return Optional.of(answer);
    }

    /** Drop every catalogued window — the flush hook for invalidation / admin cache-flush (§8.17). */
    public synchronized void clear() {
        windows.clear();
    }

    public synchronized int windowCount() {
        return windows.size();
    }

    private static final class WindowShapes {
        private final ShapeLatticeIndex index = new ShapeLatticeIndex();
        private final Set<QueryShape> shapes = new HashSet<>();
    }
}
