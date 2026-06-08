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
        Path goldenPath = locateGoldenSparkJson();
        assumeTrue(goldenPath != null, "golden spark.json not found relative to module; skipping golden diff");

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
        goldenFlattenedConfiguration = flattened;

        // The reference knobs that produced the golden spark.json: 18 GiB, 18 cores, dedicated pool, mixed.
        derivedReferenceConfiguration = DERIVER.deriveSparkConfigurationFromThreeKnobs(
                18, 18, ExecutorPlacement.DEDICATED_NODE_POOL, WorkloadType.MIXED);
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
    void derivationReproducesEveryGoldenKeyItOwnsExactly() {
        // For every key the derivation emits that the golden file also defines, the values must match.
        for (Map.Entry<String, String> derivedEntry : derivedReferenceConfiguration.entries().entrySet()) {
            String key = derivedEntry.getKey();
            if (goldenFlattenedConfiguration.containsKey(key)) {
                assertThat(derivedEntry.getValue())
                        .as("derived value for %s must match golden spark.json", key)
                        .isEqualTo(goldenFlattenedConfiguration.get(key));
            }
        }
    }

    @Test
    void emitsTheSpecificGoldenValuesCalledOutInTheTestingDocument() {
        assertThat(derivedReferenceConfiguration.require("spark.executor.memory")).isEqualTo("18g");
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
