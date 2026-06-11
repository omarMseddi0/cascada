package com.cascada.cache.domain.cube;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The Index Fabric's shape lattice (plan Appendix J.2): a bitset-indexed catalog of cached query
 * shapes that turns cube-subsumption candidate search from "evaluate every catalogued shape" into
 * a handful of 64-bit word operations per candidate.
 *
 * <p>How it works:
 * <ul>
 *   <li>Every group-by column name and every filter predicate string is <em>interned</em> to a small
 *       integer id the first time it is seen.</li>
 *   <li>Each registered shape stores two {@link BitSet}s: its group-by columns and its filters.</li>
 *   <li>The two set-algebra conditions of subsumption become pure bit-math:
 *       <ul>
 *         <li>"candidate group-by ⊇ query group-by" — {@code queryBits AND candidateBits == queryBits};</li>
 *         <li>"candidate filters ⊆ query filters" — {@code candidateBits AND NOT queryBits == ∅}.</li>
 *       </ul></li>
 *   <li>Surviving candidates come back ordered by Appendix I.4 cost (smallest group-by superset, then
 *       tightest filters) so the first exact-check winner is also the cheapest roll-up.</li>
 * </ul>
 *
 * <p>Correctness contract: the lattice is a <strong>pre-filter</strong>. It may return false
 * positives (a candidate that bit-math admits but the exact algebra rejects) — the caller always
 * confirms with {@link CubeSubsumptionPlanner#subsumes} (and the served frame is still gated by
 * {@code CubeConsistencyVerifier}). It never produces a false negative: a query group-by column the
 * interner has never seen cannot exist in any registered shape, so returning no candidates is exact.
 */
public final class ShapeLatticeIndex {

    private final Map<String, Integer> groupByColumnIds = new HashMap<>();
    private final Map<String, Integer> filterIds = new HashMap<>();
    private final List<IndexedShape> shapes = new ArrayList<>();

    /** Catalog one cached entry. Idempotent registration is the caller's concern (hash dedup). */
    public synchronized void register(CachedShapeEntry entry) {
        BitSet groupByBits = new BitSet();
        for (String column : entry.shape().groupBy()) {
            groupByBits.set(internId(groupByColumnIds, column));
        }
        BitSet filterBits = new BitSet();
        for (String filter : entry.shape().filters()) {
            filterBits.set(internId(filterIds, filter));
        }
        shapes.add(new IndexedShape(entry, groupByBits, filterBits));
    }

    public synchronized int size() {
        return shapes.size();
    }

    /**
     * The candidates whose bitsets admit subsumption of {@code query}, cheapest roll-up first
     * (Appendix I.4: fewest group-by columns, then most filters already applied).
     */
    public synchronized List<CachedShapeEntry> candidatesFor(QueryShape query) {
        BitSet queryGroupByBits = new BitSet();
        for (String column : query.groupBy()) {
            Integer id = groupByColumnIds.get(column);
            if (id == null) {
                // No registered shape has ever grouped by this column, so no candidate can be a
                // group-by superset of the query: the empty answer is exact, not approximate.
                return List.of();
            }
            queryGroupByBits.set(id);
        }
        BitSet queryFilterBits = new BitSet();
        for (String filter : query.filters()) {
            Integer id = filterIds.get(filter);
            if (id != null) {
                queryFilterBits.set(id);
            }
        }

        List<IndexedShape> admitted = new ArrayList<>();
        for (IndexedShape shape : shapes) {
            if (isGroupBySupersetByBits(shape.groupByBits(), queryGroupByBits)
                    && isFilterSubsetByBits(shape.filterBits(), queryFilterBits)) {
                admitted.add(shape);
            }
        }
        admitted.sort(Comparator
                .<IndexedShape>comparingInt(shape -> shape.entry().shape().groupBy().size())
                .thenComparing(shape -> -shape.entry().shape().filters().size()));
        return admitted.stream().map(IndexedShape::entry).toList();
    }

    /**
     * The full lookup: bit-math pre-filter, then the exact subsumption algebra on the (few)
     * survivors. Returns the cheapest exact subsumer, mirroring
     * {@link CubeSubsumptionPlanner#findBestSubsumingCacheEntryForQuery} at lattice speed.
     */
    public Optional<CachedShapeEntry> findBestSubsumer(QueryShape query, CubeSubsumptionPlanner planner) {
        for (CachedShapeEntry candidate : candidatesFor(query)) {
            if (planner.subsumes(candidate.shape(), query)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    private boolean isGroupBySupersetByBits(BitSet candidate, BitSet query) {
        BitSet copy = (BitSet) query.clone();
        copy.andNot(candidate);
        return copy.isEmpty();
    }

    private boolean isFilterSubsetByBits(BitSet candidate, BitSet query) {
        BitSet copy = (BitSet) candidate.clone();
        copy.andNot(query);
        return copy.isEmpty();
    }

    private int internId(Map<String, Integer> ids, String value) {
        return ids.computeIfAbsent(value, ignored -> ids.size());
    }

    private record IndexedShape(CachedShapeEntry entry, BitSet groupByBits, BitSet filterBits) {
    }
}
