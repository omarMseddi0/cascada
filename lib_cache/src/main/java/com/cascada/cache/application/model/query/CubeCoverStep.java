package com.cascada.cache.application.model.query;

/** One search step and the edge that advanced the first, second, or both covers. */
public record CubeCoverStep(CubeCoverPosition position, CubeCoverStep previous, CubeCoverEdge edge, double cost) {}
