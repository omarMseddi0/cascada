package com.cascada.cache.domain.model.cube;

/** A predicate whose value can be evaluated on a retained grouping column. */
public record CubeKeyFilter(String column, CubeKeyFilterOperator operator, String literal) {}
