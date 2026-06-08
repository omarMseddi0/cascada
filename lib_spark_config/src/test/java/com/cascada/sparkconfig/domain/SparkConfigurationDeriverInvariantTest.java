package com.cascada.sparkconfig.domain;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The derivation invariants from TESTING §2.3 — checked across a wide knob matrix so a bad split or
 * a broken core relationship can never reach a customer cluster.
 */
class SparkConfigurationDeriverInvariantTest {

    private final SparkConfigurationDeriver deriver = new SparkConfigurationDeriver();

    @Property
    void memoryBudgetIsNeverOversubscribedRegardlessOfKnobsOrGluten(
            @ForAll @IntRange(min = 2, max = 256) int randomAccessMemoryGigabytes,
            @ForAll boolean glutenOffHeapEnabled) {
        SparkMemorySplit split = deriver.deriveMemorySplit(randomAccessMemoryGigabytes, glutenOffHeapEnabled);
        assertThat(split.totalGigabytes()).isLessThanOrEqualTo(randomAccessMemoryGigabytes);
        assertThat(split.heapGigabytes()).isPositive();
    }

    @Property
    void limitCoresIsAlwaysExactlyOneAboveExecutorCores(
            @ForAll @IntRange(min = 1, max = 64) int processorCoreCount) {
        SparkConfiguration configuration = deriver.deriveSparkConfigurationFromThreeKnobs(
                16, processorCoreCount, ExecutorPlacement.SPREAD_ACROSS_NODES, WorkloadType.MIXED);
        int cores = Integer.parseInt(configuration.require("spark.executor.cores"));
        int limitCores = Integer.parseInt(configuration.require("spark.kubernetes.executor.limit.cores"));
        assertThat(limitCores).isEqualTo(cores + 1);
    }

    @Property
    void parallelismKeysAlwaysAgree(
            @ForAll @IntRange(min = 4, max = 256) int randomAccessMemoryGigabytes,
            @ForAll @IntRange(min = 1, max = 64) int processorCoreCount) {
        SparkConfiguration configuration = deriver.deriveSparkConfigurationFromThreeKnobs(
                randomAccessMemoryGigabytes, processorCoreCount, ExecutorPlacement.DEDICATED_NODE_POOL,
                WorkloadType.MIXED);
        assertThat(configuration.require("spark.default.parallelism"))
                .isEqualTo(configuration.require("spark.sql.shuffle.partitions"));
    }

    @Property
    void dynamicAllocationBoundsAreOrderedAndComeFromThePlacement(
            @ForAll ExecutorPlacement placement) {
        SparkConfiguration configuration = deriver.deriveSparkConfigurationFromThreeKnobs(
                16, 8, placement, WorkloadType.MIXED);
        int minimum = Integer.parseInt(configuration.require("spark.dynamicAllocation.minExecutors"));
        int maximum = Integer.parseInt(configuration.require("spark.dynamicAllocation.maxExecutors"));
        assertThat(minimum).isEqualTo(placement.minimumExecutors());
        assertThat(maximum).isEqualTo(placement.maximumExecutors());
        assertThat(minimum).isLessThanOrEqualTo(maximum);
    }

    @Test
    void adaptiveQueryExecutionAndArrowAreAlwaysOn() {
        SparkConfiguration configuration = deriver.deriveSparkConfigurationFromThreeKnobs(
                32, 8, ExecutorPlacement.SPOT_OK, WorkloadType.INTERACTIVE_DASHBOARD);
        assertThat(configuration.require("spark.sql.adaptive.enabled")).isEqualTo("true");
        assertThat(configuration.require("spark.sql.adaptive.skewJoin.enabled")).isEqualTo("true");
        assertThat(configuration.require("spark.sql.adaptive.coalescePartitions.enabled")).isEqualTo("true");
        assertThat(configuration.require("spark.sql.adaptive.localShuffleReader.enabled")).isEqualTo("true");
        assertThat(configuration.require("spark.sql.execution.arrow.pyspark.enabled")).isEqualTo("true");
    }

    @Test
    void glutenWorkloadSizesOffHeapWithoutOversubscribing() {
        SparkConfiguration configuration = deriver.deriveSparkConfigurationFromThreeKnobs(
                32, 8, ExecutorPlacement.DEDICATED_NODE_POOL, WorkloadType.BATCH_TRANSFORM);
        assertThat(configuration.containsKey("spark.memory.offHeap.size")).isTrue();
        SparkMemorySplit split = deriver.deriveMemorySplit(32, true);
        assertThat(split.offHeapGigabytes()).isPositive();
        assertThat(split.totalGigabytes()).isLessThanOrEqualTo(32);
    }

    @Test
    void rejectsNonPositiveCoreCountAndRam() {
        assertThatThrownBy(() -> deriver.deriveSparkConfigurationFromThreeKnobs(
                16, 0, ExecutorPlacement.SPREAD_ACROSS_NODES, WorkloadType.MIXED))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> deriver.deriveMemorySplit(0, false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
