package com.cascada.cache.domain.cube;

import com.cascada.cache.domain.frame.ColumnType;

import java.util.List;
import java.util.Map;

record CubeExpectedSchema(List<String> names, Map<String, ColumnType> types) {
}
