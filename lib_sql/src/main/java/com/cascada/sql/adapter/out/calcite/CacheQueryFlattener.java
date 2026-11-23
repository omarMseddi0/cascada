package com.cascada.sql.adapter.out.calcite;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Flattens distributive aggregates over a derived table when its grouping scope is preserved. */
final class CacheQueryFlattener {

    private static final Pattern DERIVED_SOURCE = Pattern.compile("(?is)\\bFROM\\s*\\(");
    private static final Set<String> DISTRIBUTIVE_FUNCTIONS = Set.of("SUM", "MIN", "MAX");

    private CacheQueryFlattener() {
    }

    static String flatten(String sql) {
        if (!DERIVED_SOURCE.matcher(sql).find()) {
            return sql;
        }
        SqlNode parsed = CalciteSql.parseQuery(sql);
        SqlNode root = parsed instanceof SqlOrderBy order ? order.query : parsed;
        if (!(root instanceof SqlSelect outer) || outer.isDistinct() || outer.getWhere() != null
                || outer.getHaving() != null) {
            return sql;
        }
        SqlSelect inner = findInnerSelect(outer.getFrom());
        if (inner == null || dropsGroupingScope(outer, inner)) {
            return sql;
        }

        Map<String, SqlNode> innerProjections = indexProjections(inner);
        if (innerProjections.isEmpty()) {
            return sql;
        }
        SqlNodeList groups = rewriteGroups(outer, inner, innerProjections);
        if (outer.getGroup() != null && groups == null) {
            return sql;
        }
        SqlNodeList output = rewriteProjections(outer, innerProjections);
        if (output == null) {
            return sql;
        }

        outer.setSelectList(output);
        outer.setFrom(inner.getFrom());
        outer.setWhere(inner.getWhere());
        outer.setGroupBy(groups);
        return CalciteSql.unparse(parsed);
    }

    private static SqlSelect findInnerSelect(SqlNode source) {
        if (source instanceof SqlCall alias && alias.getKind() == SqlKind.AS && alias.operandCount() > 2) {
            return null;
        }
        if (!(SqlTrees.stripAlias(source) instanceof SqlSelect inner) || inner.isDistinct()
                || inner.getHaving() != null || inner.getFetch() != null || inner.getOffset() != null
                || inner.getOrderList() != null && !inner.getOrderList().isEmpty()
                || !(SqlTrees.stripAlias(inner.getFrom()) instanceof SqlIdentifier)) {
            return null;
        }
        return inner;
    }

    private static boolean dropsGroupingScope(SqlSelect outer, SqlSelect inner) {
        // A scalar aggregate over grouped rows needs the original SQL to preserve row count
        // and the result for no input rows, such as SUM of an empty set of group counts.
        return (outer.getGroup() == null || outer.getGroup().isEmpty())
                && inner.getGroup() != null && !inner.getGroup().isEmpty()
                && outer.getSelectList().stream().anyMatch(SqlTrees::containsAggregate);
    }

    private static Map<String, SqlNode> indexProjections(SqlSelect inner) {
        Map<String, SqlNode> projections = new HashMap<>();
        for (SqlNode item : inner.getSelectList()) {
            SqlNode expression = SqlTrees.unaliased(item);
            if (expression == item && (!(item instanceof SqlIdentifier identifier) || identifier.isStar())) {
                return Map.of();
            }
            if (projections.putIfAbsent(key(SqlTrees.outputName(item)), expression) != null) {
                return Map.of();
            }
        }
        return projections;
    }

    private static SqlNodeList rewriteGroups(SqlSelect outer, SqlSelect inner, Map<String, SqlNode> projections) {
        if (outer.getGroup() == null) {
            return inner.getGroup();
        }
        Set<String> innerGroups = new HashSet<>();
        if (inner.getGroup() != null) {
            inner.getGroup().forEach(group -> innerGroups.add(CalciteSql.unparse(group)));
        }
        SqlNodeList rewritten = new SqlNodeList(outer.getGroup().getParserPosition());
        for (SqlNode group : outer.getGroup()) {
            if (!(group instanceof SqlIdentifier identifier)) {
                return null;
            }
            SqlNode expression = lookup(identifier, projections);
            if (expression == null || !innerGroups.contains(CalciteSql.unparse(expression))) {
                return null;
            }
            rewritten.add(expression.clone(expression.getParserPosition()));
        }
        return rewritten;
    }

    private static SqlNodeList rewriteProjections(SqlSelect outer, Map<String, SqlNode> projections) {
        SqlNodeList output = new SqlNodeList(outer.getSelectList().getParserPosition());
        for (SqlNode item : outer.getSelectList()) {
            SqlNode expression = SqlTrees.unaliased(item);
            SqlNode rewritten = rewriteExpression(expression, projections);
            if (rewritten == null) {
                return null;
            }
            SqlIdentifier alias = null;
            if (expression != item) {
                alias = ((SqlCall) item).operand(1);
            } else if (expression instanceof SqlIdentifier identifier) {
                alias = new SqlIdentifier(SqlTrees.lastName(identifier), identifier.getParserPosition());
            } else if (expression != rewritten) {
                // Spark's implicit result name comes from the original expression.
                return null;
            }
            output.add(alias == null ? rewritten
                    : SqlStdOperatorTable.AS.createCall(item.getParserPosition(), rewritten, alias));
        }
        return output;
    }

    private static SqlNode rewriteExpression(SqlNode node, Map<String, SqlNode> projections) {
        if (node instanceof SqlIdentifier identifier) {
            SqlNode expression = lookup(identifier, projections);
            return expression == null ? null : expression.clone(expression.getParserPosition());
        }
        if (node instanceof SqlLiteral) {
            return node;
        }
        if (!(node instanceof SqlBasicCall call)) {
            return null;
        }
        String function = functionName(call);
        if (DISTRIBUTIVE_FUNCTIONS.contains(function)) {
            return rewriteAggregate(call, function, projections);
        }
        if (!SqlTrees.ARITHMETIC.contains(call.getKind())) {
            return null;
        }
        SqlNode[] operands = new SqlNode[call.operandCount()];
        for (int index = 0; index < operands.length; index++) {
            operands[index] = rewriteExpression(call.operand(index), projections);
            if (operands[index] == null) {
                return null;
            }
        }
        return call.getOperator().createCall(call.getParserPosition(), operands);
    }

    private static SqlNode rewriteAggregate(SqlBasicCall outer, String function, Map<String, SqlNode> projections) {
        if (outer.getFunctionQuantifier() != null || outer.operandCount() != 1
                || !(outer.operand(0) instanceof SqlIdentifier identifier)) {
            return null;
        }
        if (outer.getOperator() instanceof SqlFunction operator
                && operator.getSqlIdentifier() != null && !operator.getSqlIdentifier().isSimple()) {
            return null;
        }
        SqlNode expression = lookup(identifier, projections);
        if (!(expression instanceof SqlBasicCall inner) || inner.getFunctionQuantifier() != null) {
            return null;
        }
        String ingredient = functionName(inner);
        if (function.equals(ingredient) || function.equals("SUM") && ingredient.equals("COUNT")) {
            return inner.clone(inner.getParserPosition());
        }
        return null;
    }

    private static SqlNode lookup(SqlIdentifier column, Map<String, SqlNode> projections) {
        return projections.get(key(SqlTrees.lastName(column)));
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String functionName(SqlBasicCall aggregate) {
        return aggregate.getOperator().getName().toUpperCase(Locale.ROOT);
    }
}
