package com.cascada.cache.domain.model.cube;

/** Identity shared across independent aggregate families. */
public record CubePopulation(ExactCubeSource source, String timeColumn, String predicate, String input) {}
