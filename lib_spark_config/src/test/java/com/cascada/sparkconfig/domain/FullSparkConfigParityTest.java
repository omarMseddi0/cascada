package com.cascada.sparkconfig.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * FULL parity gate: proves the Java module reproduces <em>every</em> key/value of
 * {@code data_collector/src/spark.json} — not just the derived subset. The assembled full
 * configuration ({@link StaticSparkInfrastructureSettings} + {@link SparkConfigurationDeriver}) must
 * contain each golden key with the exact golden value; the only keys it may have <em>beyond</em> the
 * golden file are the placement node-selector hints the derivation adds. This is the answer to "are all
 * spark.json sections the same in the Java module?" — by construction and assertion, yes.
 */
class FullSparkConfigParityTest {

    private static final SparkConfigurationDeriver DERIVER = new SparkConfigurationDeriver();
    private static final SparkSessionConfigurationAssembler ASSEMBLER = new SparkSessionConfigurationAssembler();

    private static Map<String, String> goldenFlattened;
    private static SparkConfiguration assembledFull;

    @BeforeAll
    static void loadGoldenAndAssemble() throws IOException {
        Path goldenPath = locateGoldenSparkJson();
        assumeTrue(goldenPath != null, "golden spark.json not found relative to module; skipping full parity");

        JsonNode root = new ObjectMapper().readTree(Files.readString(goldenPath));
        Map<String, String> flattened = new TreeMap<>();
        Iterator<Map.Entry<String, JsonNode>> groups = root.fields();
        while (groups.hasNext()) {
            JsonNode group = groups.next().getValue();
            Iterator<Map.Entry<String, JsonNode>> leaves = group.fields();
            while (leaves.hasNext()) {
                Map.Entry<String, JsonNode> leaf = leaves.next();
                flattened.put(leaf.getKey(), leaf.getValue().asText());
            }
        }
        goldenFlattened = flattened;

        SparkConfiguration derived = DERIVER.deriveSparkConfigurationFromThreeKnobs(
                18, 18, ExecutorPlacement.DEDICATED_NODE_POOL, WorkloadType.MIXED);
        assembledFull = ASSEMBLER.assembleFullConfiguration(derived);
    }

    private static Path locateGoldenSparkJson() {
        Path[] candidates = {
                Path.of("..", "..", "data_collector", "src", "spark.json"),
                Path.of("..", "data_collector", "src", "spark.json"),
                Path.of("data_collector", "src", "spark.json")
        };
        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    @Test
    void everyGoldenKeyIsReproducedWithTheExactValue() {
        for (Map.Entry<String, String> golden : goldenFlattened.entrySet()) {
            assertThat(assembledFull.get(golden.getKey()))
                    .as("assembled full config must contain golden key %s = %s", golden.getKey(), golden.getValue())
                    .contains(golden.getValue());
        }
    }

    @Test
    void theAssembledConfigAddsOnlyPlacementNodeSelectorBeyondTheGoldenFile() {
        for (String key : assembledFull.entries().keySet()) {
            if (!goldenFlattened.containsKey(key)) {
                assertThat(key)
                        .as("the only keys beyond spark.json should be placement node-selectors")
                        .startsWith("spark.kubernetes.node.selector.");
            }
        }
    }

    @Test
    void everySparkJsonSectionIsRepresented() {
        // A representative key from each of the 13 spark.json sections must be present.
        assertThat(assembledFull.containsKey("spark.submit.deployMode")).isTrue();          // common
        assertThat(assembledFull.containsKey("spark.kubernetes.executor.podTemplateFile")).isTrue(); // pod template
        assertThat(assembledFull.containsKey("spark.hadoop.dfs.client.read.shortcircuit")).isTrue(); // hdfs
        assertThat(assembledFull.containsKey("spark.hadoop.dfs.permissions")).isTrue();      // hadoop perms
        assertThat(assembledFull.containsKey("spark.sql.extensions")).isTrue();              // delta
        assertThat(assembledFull.containsKey("spark.metrics.namespace")).isTrue();           // metrics
        assertThat(assembledFull.containsKey("spark.dynamicAllocation.executorIdleTimeout")).isTrue(); // dyn alloc
        assertThat(assembledFull.containsKey("spark.sql.adaptive.enabled")).isTrue();        // adaptive
        assertThat(assembledFull.containsKey("spark.sql.shuffle.partitions")).isTrue();      // shuffle
        assertThat(assembledFull.containsKey("spark.sql.hive.filesourcePartitionFileCacheSize")).isTrue(); // arrow
        assertThat(assembledFull.containsKey("spark.kubernetes.executor.apiPollingInterval")).isTrue(); // api
        assertThat(assembledFull.containsKey("spark.sql.parquet.compression.codec")).isTrue(); // compression
        assertThat(assembledFull.containsKey("spark.jars")).isTrue();                        // additional
    }

    @Test
    void keyMutabilityClassificationMatchesTheSessionManager() {
        assertThat(ASSEMBLER.isLiveMutable("spark.sql.shuffle.partitions")).isTrue();
        assertThat(ASSEMBLER.requiresRestart("spark.executor.memory")).isTrue();
        assertThat(ASSEMBLER.requiresRestart("spark.executor.cores")).isTrue();
        assertThat(ASSEMBLER.requiresRestart("spark.dynamicAllocation.maxExecutors")).isTrue();
        assertThat(ASSEMBLER.isLiveMutable("spark.executor.memory")).isFalse();
    }
}
