package com.cascada.spark.adapter.out.model.spark;

import com.cascada.cache.domain.frame.ColumnType;

public record SparkColumnPlan(ColumnType type, byte getter) { }
