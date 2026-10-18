package com.cascada.spark.adapter.out.spark.model;

import com.cascada.cache.domain.frame.ColumnType;

public record SparkColumnPlan(ColumnType type, byte getter) { }
