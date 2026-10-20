package com.cascada.sparkconfig.application.port.in;

import com.cascada.sparkconfig.domain.ExecutorPlacement;
import com.cascada.sparkconfig.domain.model.SparkConfiguration;
import com.cascada.sparkconfig.domain.model.SparkMemorySplit;
import com.cascada.sparkconfig.domain.WorkloadType;

public interface DeriveSparkConfigurationUseCase {
    SparkMemorySplit deriveMemorySplit(int randomAccessMemoryGigabytes, boolean glutenOffHeapEnabled);

    SparkConfiguration deriveSparkConfigurationFromThreeKnobs(int randomAccessMemoryGigabytes,
            int processorCoreCount, ExecutorPlacement executorPlacement, WorkloadType workloadType);
}
