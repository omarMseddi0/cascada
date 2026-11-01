package com.cascada.cache.domain.model.query;

import com.cascada.cache.domain.query.ArithmeticOperator;
import java.util.Objects;

public record FormulaBinary(ArithmeticOperator operator, CompositeFormula left, CompositeFormula right)
        implements CompositeFormula {
    public FormulaBinary {
        Objects.requireNonNull(operator);
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);
    }
}
