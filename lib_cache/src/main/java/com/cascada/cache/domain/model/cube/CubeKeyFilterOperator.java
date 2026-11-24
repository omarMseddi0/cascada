package com.cascada.cache.domain.model.cube;

public enum CubeKeyFilterOperator {
    STRING_EQUALS,
    IS_NULL,
    IS_NOT_NULL,
    INTEGER_EQUALS,
    INTEGER_NOT_EQUALS,
    INTEGER_LESS,
    INTEGER_LESS_OR_EQUAL,
    INTEGER_GREATER,
    INTEGER_GREATER_OR_EQUAL;

    public boolean isIntegralComparison() {
        return switch (this) {
            case STRING_EQUALS, IS_NULL, IS_NOT_NULL -> false;
            default -> true;
        };
    }

    public boolean matchesInteger(long value, long literal) {
        return switch (this) {
            case INTEGER_EQUALS -> value == literal;
            case INTEGER_NOT_EQUALS -> value != literal;
            case INTEGER_LESS -> value < literal;
            case INTEGER_LESS_OR_EQUAL -> value <= literal;
            case INTEGER_GREATER -> value > literal;
            case INTEGER_GREATER_OR_EQUAL -> value >= literal;
            default -> throw new IllegalStateException("not an integral comparison");
        };
    }
}
