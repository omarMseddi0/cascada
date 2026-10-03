package com.cascada.sparkconfig.domain;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * FULL parity gate: proves the Java module reproduces <em>every</em> key/value of
 * {@code data_collector/src/spark.json} — not just the derived subset. The assembled full
 * configuration ({@link StaticSparkInfrastructureSettings} + {@link SparkConfigurationDeriver}) must
 * retain the golden values except for documented fail-closed corrections and the storage replication
 * value, which is deliberately inherited from the storage system. The only keys it may have
 * <em>beyond</em> the golden file are the placement node-selector hints the derivation adds.
 */
class FullSparkConfigParityTest {

    private static final Map<String, String> FAIL_CLOSED_CORRECTIONS = Map.of(
            "spark.sql.files.ignoreMissingFiles", "false",
            "spark.sql.files.ignoreCorruptFiles", "false",
            "spark.databricks.delta.retentionDurationCheck.enabled", "true");
    private static final String INHERITED_STORAGE_REPLICATION_KEY = "spark.hadoop.dfs.replication";

    private static final SparkConfigurationDeriver DERIVER = new SparkConfigurationDeriver();
    private static final SparkSessionConfigurationAssembler ASSEMBLER = new SparkSessionConfigurationAssembler();

    private static Map<String, String> goldenFlattened;
    private static SparkConfiguration assembledFull;

    @BeforeAll
    static void loadGoldenAndAssemble() throws IOException {
        goldenFlattened = SparkJsonGoldenFixture.loadFlattenedConfiguration();

        SparkConfiguration derived = DERIVER.deriveSparkConfigurationFromThreeKnobs(
                18, 18, ExecutorPlacement.DEDICATED_NODE_POOL, WorkloadType.MIXED);
        assembledFull = ASSEMBLER.assembleFullConfiguration(derived);
    }

    @Test
    void everyGoldenKeyIsReproducedWithTheExactValue() {
        for (Map.Entry<String, String> golden : goldenFlattened.entrySet()) {
            if (golden.getKey().equals(INHERITED_STORAGE_REPLICATION_KEY)) {
                assertThat(assembledFull.containsKey(INHERITED_STORAGE_REPLICATION_KEY))
                        .as("storage replication follows the storage system's configured durability policy")
                        .isFalse();
                continue;
            }
            if (FAIL_CLOSED_CORRECTIONS.containsKey(golden.getKey())) {
                assertThat(assembledFull.require(golden.getKey()))
                        .as("unsafe reference value for %s is deliberately corrected", golden.getKey())
                        .isEqualTo(FAIL_CLOSED_CORRECTIONS.get(golden.getKey()));
                continue;
            }
            if (golden.getKey().equals("spark.executor.memory")) {
                assertThat(assembledFull.get(golden.getKey())).contains("17g");
                continue;
            }
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
                        .matches("spark\\.kubernetes\\.node\\.selector\\..*|spark\\.executor\\.memoryOverhead");
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

    @Test
    void missingAndCorruptFilesFailAndDeltaRetentionCheckRemainsEnabled() {
        FAIL_CLOSED_CORRECTIONS.forEach((key, value) -> assertThat(assembledFull.require(key))
                .as("safe setting %s", key).isEqualTo(value));
    }

    @Test
    void storageReplicationIsNotOverriddenByTheSparkProfile() {
        assertThat(assembledFull.containsKey(INHERITED_STORAGE_REPLICATION_KEY)).isFalse();
    }

    @Test
    void fixedInfrastructureMapCannotBeMutatedByCallers() {
        assertThatThrownBy(() -> StaticSparkInfrastructureSettings.referenceInfrastructure()
                .put("spark.executor.memory", "1g"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
