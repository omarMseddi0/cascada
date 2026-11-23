package com.cascada.cache.application.model.query;

import com.cascada.cache.domain.model.cube.CubeMeasure;
import java.util.List;
import java.util.Map;

public record ExactCubePlan(Map<CubeMeasure, List<CubeCoverEdge>> covers) {
}
