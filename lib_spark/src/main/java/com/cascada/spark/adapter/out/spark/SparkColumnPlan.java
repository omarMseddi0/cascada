package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.domain.frame.ColumnType;

record SparkColumnPlan(ColumnType type, byte getter) { }
