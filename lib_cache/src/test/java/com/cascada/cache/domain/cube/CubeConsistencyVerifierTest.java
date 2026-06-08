package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the cube's data-inconsistency guard ({@link CubeConsistencyVerifier}): a faithful roll-up is
 * confirmed, while every way a roll-up could silently return a wrong number — a tampered cell, a
 * fabricated/missing group, a missing AVG ingredient, a holistic aggregate, a grouped column the frame
 * does not carry, or a non-numeric measure — is caught and reported as inconsistent so the engine
 * bypasses to Spark instead of serving the bad answer.
 */
class CubeConsistencyVerifierTest {

    private final CubeSubsumptionPlanner planner = new CubeSubsumptionPlanner();
    private final CubeConsistencyVerifier verifier = new CubeConsistencyVerifier();

    /** Finest cached grain (appName, deviceType, region) with SUM+COUNT so AVG can be reconstructed. */
    private ResultFrame finestFrame() {
        return ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("deviceType", ColumnType.STRING)
                .column("region", ColumnType.STRING)
                .column("SUM(latency)", ColumnType.DOUBLE)
                .column("COUNT(latency)", ColumnType.DOUBLE)
                .row(row("netflix", "mobile", "eu", 100.0, 4.0))
                .row(row("netflix", "tablet", "eu", 50.0, 1.0))
                .row(row("netflix", "mobile", "us", 30.0, 2.0))
                .row(row("youtube", "mobile", "eu", 20.0, 2.0))
                .build();
    }

    private Map<String, Object> row(String app, String device, String region, double sum, double count) {
        Map<String, Object> values = new HashMap<>();
        values.put("appName", app);
        values.put("deviceType", device);
        values.put("region", region);
        values.put("SUM(latency)", sum);
        values.put("COUNT(latency)", count);
        return values;
    }

    private CachedShapeEntry finest() {
        return new CachedShapeEntry(
                new QueryShape(Set.of("appName", "deviceType", "region"), Set.of(),
                        Set.of("SUM(latency)", "COUNT(latency)")),
                finestFrame());
    }

    @Test
    void confirmsAFaithfulSumRollUp() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)"));
        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);

        CubeConsistencyVerifier.VerificationResult result = verifier.verifyRollUp(finest(), query, answer);
        assertThat(result.isConsistent()).isTrue();
        assertThat(result.reason()).isEmpty();
    }

    @Test
    void confirmsAFaithfulAverageReconstruction() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(),
                Set.of("SUM(latency)", "COUNT(latency)", "AVG(latency)"));
        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);

        assertThat(verifier.verifyRollUp(finest(), query, answer).isConsistent()).isTrue();
    }

    @Test
    void confirmsAFaithfulFilterDown() {
        QueryShape query = new QueryShape(Set.of("appName"),
                Set.of("region IN ('eu', 'us')"), Set.of("SUM(latency)"));
        ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);

        assertThat(verifier.verifyRollUp(finest(), query, answer).isConsistent()).isTrue();
    }

    @Test
    void catchesATamperedMeasureCell() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)"));
        ResultFrame correct = planner.rollUpAndFilterDown(finest(), query);
        ResultFrame tampered = withMeasureBumped(correct, "SUM(latency)");

        CubeConsistencyVerifier.VerificationResult result = verifier.verifyRollUp(finest(), query, tampered);
        assertThat(result.isConsistent()).isFalse();
        assertThat(result.reason()).contains("SUM(latency)");
    }

    @Test
    void catchesAFabricatedExtraGroup() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)"));
        ResultFrame correct = planner.rollUpAndFilterDown(finest(), query);
        ResultFrame withGhostGroup = withExtraRow(correct,
                Map.of("appName", "ghostApp", "SUM(latency)", 1.0));

        assertThat(verifier.verifyRollUp(finest(), query, withGhostGroup).isConsistent()).isFalse();
    }

    @Test
    void catchesAMissingGroup() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)"));
        ResultFrame correct = planner.rollUpAndFilterDown(finest(), query);
        ResultFrame missingARow = withFirstRowDropped(correct);

        CubeConsistencyVerifier.VerificationResult result = verifier.verifyRollUp(finest(), query, missingARow);
        assertThat(result.isConsistent()).isFalse();
        assertThat(result.reason()).contains("row count mismatch");
    }

    @Test
    void rejectsAverageWhenCandidateFrameLacksTheCountIngredient() {
        // Frame stores only SUM; an AVG roll-up cannot be verified and must bypass.
        CachedShapeEntry sumOnly = new CachedShapeEntry(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)")),
                ResultFrame.builder()
                        .column("appName", ColumnType.STRING)
                        .column("SUM(latency)", ColumnType.DOUBLE)
                        .row(Map.of("appName", "netflix", "SUM(latency)", 180.0))
                        .build());
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("AVG(latency)"));

        ResultFrame anyAnswer = ResultFrame.builder()
                .column("appName", ColumnType.STRING)
                .column("AVG(latency)", ColumnType.DOUBLE)
                .row(Map.of("appName", "netflix", "AVG(latency)", 45.0))
                .build();

        CubeConsistencyVerifier.VerificationResult result = verifier.verifyRollUp(sumOnly, query, anyAnswer);
        assertThat(result.isConsistent()).isFalse();
        assertThat(result.reason()).contains("COUNT");
    }

    @Test
    void rejectsHolisticAggregatesOutright() {
        QueryShape query = new QueryShape(Set.of("appName"), Set.of(), Set.of("COUNT(DISTINCT userId)"));
        CubeConsistencyVerifier.VerificationResult result =
                verifier.verifyRollUp(finest(), query, ResultFrame.empty());
        assertThat(result.isConsistent()).isFalse();
        assertThat(result.reason()).contains("holistic");
    }

    @Test
    void rejectsAQueryGroupingByAColumnTheFrameDoesNotCarry() {
        QueryShape query = new QueryShape(Set.of("country"), Set.of(), Set.of("SUM(latency)"));
        CubeConsistencyVerifier.VerificationResult result =
                verifier.verifyRollUp(finest(), query, ResultFrame.empty());
        assertThat(result.isConsistent()).isFalse();
        assertThat(result.reason()).contains("country");
    }

    @Test
    void verifierAgreesWithPlannerAcrossManyShapes() {
        // A faithful planner roll-up is consistent for every shape the planner says it subsumes.
        List<QueryShape> queries = List.of(
                new QueryShape(Set.of("appName"), Set.of(), Set.of("SUM(latency)")),
                new QueryShape(Set.of("appName", "region"), Set.of(), Set.of("SUM(latency)", "COUNT(latency)")),
                new QueryShape(Set.of(), Set.of(), Set.of("SUM(latency)")),
                new QueryShape(Set.of("appName"), Set.of("region = 'eu'"), Set.of("SUM(latency)")),
                new QueryShape(Set.of("appName"), Set.of(), Set.of("AVG(latency)")));
        for (QueryShape query : queries) {
            assertThat(planner.subsumes(finest().shape(), query))
                    .as("planner should subsume %s", query)
                    .isTrue();
            ResultFrame answer = planner.rollUpAndFilterDown(finest(), query);
            assertThat(verifier.verifyRollUp(finest(), query, answer).isConsistent())
                    .as("verifier should confirm faithful roll-up for %s", query)
                    .isTrue();
        }
    }

    // --- frame-tampering helpers (simulate a buggy roll-up) ------------------------------------------

    private ResultFrame withMeasureBumped(ResultFrame frame, String measure) {
        ResultFrame.Builder builder = rebuildSchema(frame);
        boolean bumped = false;
        for (Map<String, Object> r : frame.rows()) {
            Map<String, Object> copy = new HashMap<>(r);
            if (!bumped) {
                copy.put(measure, ((Number) r.get(measure)).doubleValue() + 1.0);
                bumped = true;
            }
            builder.row(copy);
        }
        return builder.build();
    }

    private ResultFrame withExtraRow(ResultFrame frame, Map<String, Object> extra) {
        ResultFrame.Builder builder = rebuildSchema(frame);
        frame.rows().forEach(builder::row);
        builder.row(new HashMap<>(extra));
        return builder.build();
    }

    private ResultFrame withFirstRowDropped(ResultFrame frame) {
        ResultFrame.Builder builder = rebuildSchema(frame);
        List<Map<String, Object>> rows = new ArrayList<>(frame.rows());
        rows.remove(0);
        rows.forEach(builder::row);
        return builder.build();
    }

    private ResultFrame.Builder rebuildSchema(ResultFrame frame) {
        ResultFrame.Builder builder = ResultFrame.builder();
        for (String column : frame.columnNames()) {
            builder.column(column, frame.columnType(column));
        }
        return builder;
    }
}
