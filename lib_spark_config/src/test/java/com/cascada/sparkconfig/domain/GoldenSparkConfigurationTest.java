package com.cascada.sparkconfig.domain;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Spark-config golden gate (ARCHITECTURE §8, TESTING §1.5): the pure derivation must reproduce
 * the hand-tuned {@code data_collector/src/spark.json} for the reference knobs, key-by-key. A drift
 * in either the derivation or the golden file fails the build, forcing an explicit review.
 */
class GoldenSparkConfigurationTest {

    private static final SparkConfigurationDeriver DERIVER = new SparkConfigurationDeriver();

    private static Map<String, String> goldenFlattenedConfiguration;
    private static SparkConfiguration derivedReferenceConfiguration;

    @BeforeAll
    static void loadGoldenAndDeriveReference() throws IOException {
        goldenFlattenedConfiguration = SparkJsonGoldenFixture.loadFlattenedConfiguration();

        // The reference knobs that produced the golden spark.json: 18 GiB, 18 cores, dedicated pool, mixed.
        derivedReferenceConfiguration = DERIVER.deriveSparkConfigurationFromThreeKnobs(
                18, 18, ExecutorPlacement.DEDICATED_NODE_POOL, WorkloadType.MIXED);
    }

    @Test
    void derivationReproducesEveryGoldenKeyItOwnsExactly() {
        // For every key the derivation emits that the golden file also defines, the values must match.
        for (Map.Entry<String, String> derivedEntry : derivedReferenceConfiguration.entries().entrySet()) {
            String key = derivedEntry.getKey();
            if (key.equals("spark.executor.memory") || key.equals("spark.executor.memoryOverhead")) {
                continue; // intentional safety correction: heap + explicit overhead must fit the knob
            }
            if (goldenFlattenedConfiguration.containsKey(key)) {
                assertThat(derivedEntry.getValue())
                        .as("derived value for %s must match golden spark.json", key)
                        .isEqualTo(goldenFlattenedConfiguration.get(key));
            }
        }
    }

    @Test
    void emitsTheSpecificGoldenValuesCalledOutInTheTestingDocument() {
        assertThat(derivedReferenceConfiguration.require("spark.executor.memory")).isEqualTo("17g");
        assertThat(derivedReferenceConfiguration.require("spark.executor.memoryOverhead")).isEqualTo("1g");
        assertThat(derivedReferenceConfiguration.require("spark.executor.cores")).isEqualTo("18");
        assertThat(derivedReferenceConfiguration.require("spark.kubernetes.executor.limit.cores")).isEqualTo("19");
        assertThat(derivedReferenceConfiguration.require("spark.driver.memory")).isEqualTo("2g");
        assertThat(derivedReferenceConfiguration.require("spark.default.parallelism")).isEqualTo("254");
        assertThat(derivedReferenceConfiguration.require("spark.sql.shuffle.partitions")).isEqualTo("254");
        assertThat(derivedReferenceConfiguration.require("spark.dynamicAllocation.minExecutors")).isEqualTo("2");
        assertThat(derivedReferenceConfiguration.require("spark.dynamicAllocation.maxExecutors")).isEqualTo("3");
        assertThat(derivedReferenceConfiguration.require("spark.sql.adaptive.enabled")).isEqualTo("true");
        assertThat(derivedReferenceConfiguration.require("spark.sql.files.maxPartitionBytes")).isEqualTo("267108864");
        assertThat(derivedReferenceConfiguration.require("spark.sql.extensions"))
                .isEqualTo("io.delta.sql.DeltaSparkSessionExtension");
    }

    @Test
    void referenceProfileDoesNotExposeOffHeapToTheCustomer() {
        // The golden config has no offHeap key; the MIXED reference must not emit one either.
        assertThat(derivedReferenceConfiguration.containsKey("spark.memory.offHeap.size")).isFalse();
        assertThat(goldenFlattenedConfiguration).doesNotContainKey("spark.memory.offHeap.size");
    }
}
