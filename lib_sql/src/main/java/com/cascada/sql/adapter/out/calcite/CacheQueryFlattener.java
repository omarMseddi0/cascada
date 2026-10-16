package com.cascada.sql.adapter.out.calcite;

import java.util.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Collapses proven distributive wrappers; filters/limits that change aggregate scope stay untouched. */
final class CacheQueryFlattener {
    private static final java.util.regex.Pattern DERIVED_SOURCE = java.util.regex.Pattern.compile("(?is)\\bFROM\\s*\\(");
    private CacheQueryFlattener() { }

    static String flatten(String sql) {
        if (!DERIVED_SOURCE.matcher(sql).find()) return sql;
        SqlNode parsed = CalciteSql.parseQuery(sql);
        SqlNode root = parsed instanceof SqlOrderBy order ? order.query : parsed;
        if (!(root instanceof SqlSelect outer) || outer.isDistinct() || outer.getWhere() != null
                || outer.getHaving() != null || outer.getFrom() == null) return sql;
        SqlNode source = outer.getFrom();
        if (source.getKind() == SqlKind.AS) source = ((SqlCall) source).operand(0);
        if (!(source instanceof SqlSelect inner) || inner.isDistinct() || inner.getHaving() != null
                || inner.getFetch() != null || inner.getOffset() != null
                || inner.getOrderList() != null && !inner.getOrderList().isEmpty()
                || !(stripAlias(inner.getFrom()) instanceof SqlIdentifier)) return sql;
        Map<String, SqlNode> projected = new HashMap<>();
        for (SqlNode item : inner.getSelectList()) {
            if (item.getKind() == SqlKind.AS && item instanceof SqlCall as)
                projected.put(key(((SqlIdentifier) as.operand(1)).getSimple()), as.operand(0));
            else if (item instanceof SqlIdentifier id && !id.isStar())
                projected.put(key(last(id)), item);
            else return sql;
        }
        SqlNodeList groups = outer.getGroup() == null ? inner.getGroup() : new SqlNodeList(outer.getParserPosition());
        if (outer.getGroup() != null) {
            Set<String> innerGroups = new HashSet<>();
            if (inner.getGroup() != null) inner.getGroup().forEach(group -> innerGroups.add(CalciteSql.unparse(group)));
            for (SqlNode group : outer.getGroup()) {
                if (!(group instanceof SqlIdentifier id)) return sql;
                SqlNode mapped = projected.get(key(last(id)));
                if (mapped == null || !innerGroups.contains(CalciteSql.unparse(mapped))) return sql;
                groups.add(mapped.clone(mapped.getParserPosition()));
            }
        }
        SqlNodeList output = new SqlNodeList(outer.getParserPosition());
        for (SqlNode item : outer.getSelectList()) {
            SqlNode expression = item;
            SqlIdentifier alias = null;
            if (item.getKind() == SqlKind.AS && item instanceof SqlCall as) {
                expression = as.operand(0);
                alias = as.operand(1);
            }
            SqlNode mapped = map(expression, projected);
            if (mapped == null) return sql;
            if (alias == null && expression instanceof SqlIdentifier id)
                alias = new SqlIdentifier(last(id), id.getParserPosition());
            output.add(alias == null ? mapped : SqlStdOperatorTable.AS.createCall(item.getParserPosition(), mapped, alias));
        }
        outer.setSelectList(output);
        outer.setFrom(inner.getFrom());
        outer.setWhere(inner.getWhere());
        outer.setGroupBy(groups);
        return CalciteSql.unparse(parsed);
    }

    private static SqlNode map(SqlNode node, Map<String, SqlNode> projected) {
        if (node instanceof SqlIdentifier id) {
            SqlNode value = projected.get(key(last(id)));
            return value == null ? null : value.clone(value.getParserPosition());
        }
        if (node instanceof SqlLiteral) return node;
        if (!(node instanceof SqlBasicCall call)) return null;
        String function = call.getOperator().getName().toUpperCase(Locale.ROOT);
        if (Set.of("SUM", "MIN", "MAX").contains(function) && call.getFunctionQuantifier() == null
                && call.operandCount() == 1 && call.operand(0) instanceof SqlIdentifier id) {
            SqlNode value = projected.get(key(last(id)));
            if (!(value instanceof SqlBasicCall aggregate) || aggregate.getFunctionQuantifier() != null) return null;
            String ingredient = aggregate.getOperator().getName().toUpperCase(Locale.ROOT);
            if (function.equals(ingredient) || function.equals("SUM") && ingredient.equals("COUNT"))
                return aggregate.clone(aggregate.getParserPosition());
            return null;
        }
        if (call.getKind() != SqlKind.PLUS && call.getKind() != SqlKind.MINUS && call.getKind() != SqlKind.TIMES
                && call.getKind() != SqlKind.DIVIDE) return null;
        SqlNode[] operands = new SqlNode[call.operandCount()];
        for (int index = 0; index < operands.length; index++) {
            operands[index] = map(call.operand(index), projected);
            if (operands[index] == null) return null;
        }
        return call.getOperator().createCall(call.getParserPosition(), operands);
    }

    private static SqlNode stripAlias(SqlNode node) {
        return node != null && node.getKind() == SqlKind.AS ? ((SqlCall) node).operand(0) : node;
    }
    private static String last(SqlIdentifier id) { return id.names.get(id.names.size() - 1); }
    private static String key(String name) { return name.toLowerCase(Locale.ROOT); }
}
