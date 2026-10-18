package com.cascada.cache.domain.model.cube;

import com.cascada.cache.domain.frame.ColumnType;
import java.util.List;
import java.util.Map;

public record CubeExpectedSchema(List<String> names, Map<String, ColumnType> types) {
}
