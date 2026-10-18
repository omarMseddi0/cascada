package com.cascada.cache.domain.model.query;

import java.util.Objects;

public record FormulaColumn(String name) implements CompositeFormula {
    public FormulaColumn { Objects.requireNonNull(name); }
}
