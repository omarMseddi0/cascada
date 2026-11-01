package com.cascada.cache.application.model.cube;

import com.cascada.cache.domain.frame.ColumnType;
import java.util.List;

/** Parsed cube filter bound to its physical frame column. */
public record PreparedCubeFilter(int columnIndex, ColumnType type, List<CubeFilterLiteral> literals,
                                 boolean inFilter) {
    public PreparedCubeFilter {
        literals = List.copyOf(literals);
    }
}
