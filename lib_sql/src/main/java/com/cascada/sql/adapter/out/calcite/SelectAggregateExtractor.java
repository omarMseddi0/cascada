package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.domain.model.query.CompositeFormula;
import com.cascada.sql.adapter.out.model.calcite.aggregate.SelectAggregates;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;

/** Reads aggregate definitions and the formulas that run after the buckets have been merged. */
final class SelectAggregateExtractor {

    SelectAggregates extract(SqlSelect select) {
        List<SqlBasicCall> calls = new ArrayList<>();
        List<String> specifications = new ArrayList<>();
        Map<String, String> compositeAliases = new LinkedHashMap<>();
        Map<String, AggregateFunction> combineFunctions = new LinkedHashMap<>();
        Map<String, CompositeFormula> formulas = new LinkedHashMap<>();

        for (SqlNode item : select.getSelectList()) {
            SqlNode expression = SqlTrees.unaliased(item);
            List<SqlBasicCall> ingredients = new ArrayList<>();
            collectAggregateCalls(expression, ingredients);
            if (ingredients.isEmpty()) {
                continue;
            }
            calls.addAll(ingredients);

            String alias = expression == item ? null : SqlTrees.outputName(item);
            String rendered = CalciteSql.unparse(expression);
            boolean singleAggregate = ingredients.size() == 1 && SqlTrees.isAggregateCall(expression);
            if (singleAggregate) {
                String outputColumn = alias == null ? rendered : alias;
                // Use the parsed operator so aliases such as MAX(latency) AS peak keep their merge rule.
                AggregateFunction.fromSqlName(ingredients.getFirst().getOperator().getName())
                        .ifPresent(function -> combineFunctions.put(outputColumn, function));
            } else if (alias != null) {
                SqlNode formula = replaceAggregatesWithIngredientNames(expression);
                compositeAliases.put(alias, CalciteSql.unparse(formula));
                formulas.put(alias, CompositeFormulaCompiler.compile(formula));
                for (SqlBasicCall ingredient : ingredients) {
                    String ingredientSql = CalciteSql.unparse(ingredient);
                    String name = CompositeIngredientNames.alias(ingredientSql);
                    combineFunctions.put(name, AggregateFunction.fromSqlName(ingredient.getOperator().getName()).orElse(null));
                    specifications.add(ingredientSql + " AS " + name);
                }
            }
            specifications.add(alias == null ? rendered : rendered + " AS " + alias);
        }
        return new SelectAggregates(calls, specifications, compositeAliases, combineFunctions, formulas);
    }

    private void collectAggregateCalls(SqlNode expression, List<SqlBasicCall> calls) {
        if (!(expression instanceof SqlCall call)) {
            return;
        }
        if (call instanceof SqlBasicCall aggregate && SqlTrees.isAggregateCall(aggregate)) {
            calls.add(aggregate);
            return;
        }
        for (SqlNode operand : call.getOperandList()) {
            collectAggregateCalls(operand, calls);
        }
    }

    private SqlNode replaceAggregatesWithIngredientNames(SqlNode expression) {
        return expression.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (call instanceof SqlBasicCall aggregate && SqlTrees.isAggregateCall(aggregate)) {
                    String name = CompositeIngredientNames.alias(CalciteSql.unparse(aggregate));
                    return new SqlIdentifier(name, call.getParserPosition());
                }
                return super.visit(call);
            }
        });
    }
}
