package com.cascada.fabric.application.port.out;

import com.cascada.sparkconfig.domain.SparkMemorySplit;

@FunctionalInterface
public interface ExecutorMemorySizingPort {
    SparkMemorySplit deriveMemorySplit(int randomAccessMemoryGigabytes, boolean glutenOffHeapEnabled);
}
