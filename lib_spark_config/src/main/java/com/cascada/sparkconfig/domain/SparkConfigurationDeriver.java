package com.cascada.sparkconfig.domain;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The zero-config derivation (plan §6.6, Appendix G; ARCHITECTURE §8). A pure function that turns
 * the three knobs — RAM, CPU, placement — plus an optional workload hint into every Spark/Delta flag,
 * and reproduces the golden {@code data_collector/src/spark.json} for the reference knobs.
 *
 * <p>Being a pure function is the design's leverage: if Gluten changes its memory expectations we edit
 * one method, not ten thousand customer configs, and the golden test re-runs on every image bump.
 *
 * <p>Derivation rules of note:
 * <ul>
 *   <li>{@code spark.kubernetes.executor.limit.cores = executor.cores + 1} (golden: 19 vs 18);</li>
 *   <li>AQE and Arrow are always on (free wins; never exposed as toggles);</li>
 *   <li>the Delta extension/catalog and the advisory partition size track the table profile (golden:
 *       267108864);</li>
 *   <li>min/max executors come from the placement budget;</li>
 *   <li>off-heap is sized only when the workload uses Gluten/Velox, so a misconfiguration cannot OOM
 *       — and the customer never sees {@code offHeap}.</li>
 * </ul>
 */
public final class SparkConfigurationDeriver {

    static final long ADVISORY_PARTITION_SIZE_BYTES = 267_108_864L;
    static final String REFERENCE_DRIVER_MEMORY = "2g";
    static final int REFERENCE_SHUFFLE_PARTITIONS = 254;
    static final String DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension";
    static final String DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog";

    /** Splits one executor's RAM between heap, Gluten off-heap, and overhead (invariant: sum <= RAM). */
    public SparkMemorySplit deriveMemorySplit(int randomAccessMemoryGigabytes, boolean glutenOffHeapEnabled) {
        if (randomAccessMemoryGigabytes <= 0) {
            throw new IllegalArgumentException("RAM must be > 0 gigabytes, but was: " + randomAccessMemoryGigabytes);
        }
        if (!glutenOffHeapEnabled) {
            // Reference behaviour: the entire budget is JVM heap (matches the golden spark.json).
            return new SparkMemorySplit(randomAccessMemoryGigabytes, 0, 0);
        }
        // Reserve at least 1 GiB overhead, give the majority of the remainder to Velox off-heap.
        int overhead = Math.max(1, randomAccessMemoryGigabytes / 10);
        int remaining = randomAccessMemoryGigabytes - overhead;
        int offHeap = (remaining * 6) / 10;
        int heap = remaining - offHeap;
        if (heap <= 0) {
            heap = 1;
            offHeap = Math.max(0, randomAccessMemoryGigabytes - heap - overhead);
        }
        return new SparkMemorySplit(heap, offHeap, overhead);
    }

    /** The main entry point: the three knobs (+ workload hint) to a complete configuration. */
    public SparkConfiguration deriveSparkConfigurationFromThreeKnobs(int randomAccessMemoryGigabytes,
                                                                     int processorCoreCount,
                                                                     ExecutorPlacement executorPlacement,
                                                                     WorkloadType workloadType) {
        if (processorCoreCount <= 0) {
            throw new IllegalArgumentException("core count must be > 0, but was: " + processorCoreCount);
        }

        SparkMemorySplit memorySplit =
                deriveMemorySplit(randomAccessMemoryGigabytes, workloadType.isGlutenOffHeapEnabled());

        Map<String, String> entries = new LinkedHashMap<>();

        // --- Core executor / driver shape ---
        entries.put("spark.executor.memory", memorySplit.heapAsSparkMemoryString());
        entries.put("spark.executor.cores", Integer.toString(processorCoreCount));
        entries.put("spark.kubernetes.executor.limit.cores", Integer.toString(processorCoreCount + 1));
        entries.put("spark.driver.memory", REFERENCE_DRIVER_MEMORY);
        if (memorySplit.offHeapGigabytes() > 0) {
            entries.put("spark.memory.offHeap.enabled", "true");
            entries.put("spark.memory.offHeap.size", memorySplit.offHeapGigabytes() + "g");
        }

        // --- Delta extension / catalog / file sizing (always on; advisory size tracks the profile) ---
        entries.put("spark.sql.extensions", DELTA_EXTENSION);
        entries.put("spark.sql.catalog.spark_catalog", DELTA_CATALOG);
        entries.put("spark.sql.files.maxPartitionBytes", Long.toString(ADVISORY_PARTITION_SIZE_BYTES));
        entries.put("spark.databricks.delta.optimize.maxFileSize", Long.toString(ADVISORY_PARTITION_SIZE_BYTES));
        entries.put("spark.sql.adaptive.advisoryPartitionSizeInBytes", Long.toString(ADVISORY_PARTITION_SIZE_BYTES));

        // --- AQE always on (free wins, never a customer toggle) ---
        entries.put("spark.sql.adaptive.enabled", "true");
        entries.put("spark.sql.adaptive.skewJoin.enabled", "true");
        entries.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        entries.put("spark.sql.adaptive.localShuffleReader.enabled", "true");

        // --- Arrow always on ---
        entries.put("spark.sql.execution.arrow.pyspark.enabled", "true");

        // --- Parallelism (AQE re-coalesces at runtime, so the static baseline barely matters) ---
        entries.put("spark.default.parallelism", Integer.toString(REFERENCE_SHUFFLE_PARTITIONS));
        entries.put("spark.sql.shuffle.partitions", Integer.toString(REFERENCE_SHUFFLE_PARTITIONS));

        // --- Dynamic allocation from the placement budget ---
        entries.put("spark.dynamicAllocation.enabled", "true");
        entries.put("spark.dynamicAllocation.minExecutors", Integer.toString(executorPlacement.minimumExecutors()));
        entries.put("spark.dynamicAllocation.maxExecutors", Integer.toString(executorPlacement.maximumExecutors()));

        // --- Placement compiled to a Kubernetes node-selector hint ---
        entries.put("spark.kubernetes.node.selector." + executorPlacement.nodeSelectorLabel(),
                executorPlacement.nodeSelectorValue());

        return new SparkConfiguration(entries);
    }
}
