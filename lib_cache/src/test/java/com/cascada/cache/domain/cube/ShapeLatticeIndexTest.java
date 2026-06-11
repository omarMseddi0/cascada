package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The shape lattice (plan Appendix J.2): bit-math candidate pre-filtering must agree with the exact
 * subsumption algebra — same winner as the planner's full scan, no false negatives, cheapest-first
 * ordering — while pruning everything the bitsets can prove impossible.
 */
class ShapeLatticeIndexTest {

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();

    private CachedShapeEntry entry(Set<String> groupBy, Set<String> filters, Set<String> aggregates) {
        ResultFrame.Builder frame = ResultFrame.builder();
        groupBy.forEach(column -> frame.column(column, ColumnType.STRING));
        aggregates.forEach(aggregate -> frame.column(aggregate, ColumnType.DOUBLE));
        Map<String, Object> row = new java.util.LinkedHashMap<>();
        groupBy.forEach(column -> row.put(column, "x"));
        aggregates.forEach(aggregate -> row.put(aggregate, 1.0));
        frame.row(row);
        return new CachedShapeEntry(new QueryShape(groupBy, filters, aggregates), frame.build());
    }

    @Test
    void latticeFindsTheSameBestSubsumerAsThePlannersFullScan() {
        CachedShapeEntry coarse = entry(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(bytes)"));
        CachedShapeEntry coarser = entry(Set.of("appName", "deviceType", "region"), Set.of(), Set.of("SUM(bytes)"));
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));

        ShapeLatticeIndex lattice = new ShapeLatticeIndex();
        lattice.register(coarser);
        lattice.register(coarse);

        Optional<CachedShapeEntry> viaLattice = lattice.findBestSubsumer(query, planner);
        Optional<CachedShapeEntry> viaFullScan =
                planner.findBestSubsumingCacheEntryForQuery(query, List.of(coarser, coarse));

        assertThat(viaLattice).isPresent();
        assertThat(viaLattice.get().shape()).isEqualTo(viaFullScan.orElseThrow().shape());
        // I.4: the 2-column superset beats the 3-column one (fewer rows to roll up).
        assertThat(viaLattice.get().shape().groupBy()).isEqualTo(Set.of("appName", "deviceType"));
    }

    @Test
    void unknownGroupByColumnYieldsNoCandidatesExactly() {
        ShapeLatticeIndex lattice = new ShapeLatticeIndex();
        lattice.register(entry(Set.of("appName"), Set.of(), Set.of("SUM(bytes)")));

        QueryShape query = new QueryShape(Set.of("city"), Set.of(), Set.of("SUM(bytes)"));
        // No registered shape ever grouped by "city" -> no candidate can be a superset; empty is exact.
        assertThat(lattice.candidatesFor(query)).isEmpty();
    }

    @Test
    void candidateWithAFilterTheQueryLacksIsPrunedByBitMath() {
        CachedShapeEntry filtered =
                entry(Set.of("appName"), Set.of("region = 'EU'"), Set.of("SUM(bytes)"));
        ShapeLatticeIndex lattice = new ShapeLatticeIndex();
        lattice.register(filtered);

        // Query has no filters: the candidate's pre-applied EU filter cannot be undone -> pruned.
        QueryShape unfiltered = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)"));
        assertThat(lattice.candidatesFor(unfiltered)).isEmpty();

        // Query that carries the same filter: candidate admitted and exact algebra agrees.
        QueryShape matching = new QueryShape(Set.of("appName"), Set.of("region = 'EU'"), Set.of("SUM(bytes)"));
        assertThat(lattice.candidatesFor(matching)).hasSize(1);
        assertThat(lattice.findBestSubsumer(matching, planner)).isPresent();
    }

    @Test
    void candidatesComeBackCheapestRollUpFirst() {
        CachedShapeEntry wide = entry(Set.of("appName", "deviceType", "region"), Set.of(), Set.of("SUM(bytes)"));
        CachedShapeEntry narrow = entry(Set.of("appName", "deviceType"), Set.of(), Set.of("SUM(bytes)"));
        ShapeLatticeIndex lattice = new ShapeLatticeIndex();
        lattice.register(wide);
        lattice.register(narrow);

        List<CachedShapeEntry> candidates =
                lattice.candidatesFor(new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(bytes)")));
        assertThat(candidates).hasSize(2);
        assertThat(candidates.get(0).shape().groupBy()).hasSize(2);
        assertThat(candidates.get(1).shape().groupBy()).hasSize(3);
    }
}
