package com.cascada.cache.domain.frame;

/**
 * The value type of a {@link ResultFrame} column. The cache only deals in the three types its merge
 * math needs: epoch-second / count longs, measure doubles, and dimension strings.
 */
public enum ColumnType {
    LONG,
    DOUBLE,
    STRING
}
