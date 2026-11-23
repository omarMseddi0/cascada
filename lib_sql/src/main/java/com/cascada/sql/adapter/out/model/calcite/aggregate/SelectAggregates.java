package com.cascada.sql.adapter.out.model.calcite.aggregate;

import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.model.query.CompositeFormula;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;

/** Aggregate calls, merge operations and formulas discovered while walking a SELECT list. */
public record SelectAggregates(List<SqlBasicCall> calls,
                               List<String> specifications,
                               Map<String, String> compositeAliases,
                               Map<String, AggregateFunction> combineFunctions,
                               Map<String, CompositeFormula> formulas) {
}
