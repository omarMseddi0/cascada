package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.domain.model.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.*;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/** Resolves each SELECT scope separately, preserving the logical output names of derived relations. */
final class ScopedLogicalSqlTranslator {
    private final TableCatalog catalog;
    private final Map<String, RegisteredTable> wildcardRelations = new HashMap<>();
    private final Map<String, String> paths = new LinkedHashMap<>();
    private final Set<SqlNode> translated = Collections.newSetFromMap(new IdentityHashMap<>());
    private final String sentinelPrefix;

    ScopedLogicalSqlTranslator(TableCatalog catalog, String originalSql) {
        this.catalog = catalog;
        sentinelPrefix = SqlTrees.unusedSentinel(originalSql, "CASCADA_SCOPED_SOURCE");
    }

    String translate(SqlNode root) {
        String sql = CalciteSql.unparse(query(root, Set.of(), Map.of()));
        for (var entry : paths.entrySet()) sql = sql.replace(entry.getKey(), entry.getValue());
        return sql;
    }

    private SqlNode query(SqlNode root, Set<String> ctes, Map<String, RegisteredTable> parents) {
        if (translated.contains(root)) return root;
        if (root instanceof SqlOrderBy order) {
            // Keep ORDER BY in the same resolver scope as its SELECT.
            if (order.query instanceof SqlSelect select) {
                return select(select, order, ctes, parents);
            }
            return new SqlOrderBy(order.getParserPosition(), query(order.query, ctes, parents),
                    order.orderList, order.offset, order.fetch);
        }
        if (root instanceof SqlWith with) {
            Set<String> names = new HashSet<>(ctes);
            Map<String, RegisteredTable> previousMappings = new HashMap<>(wildcardRelations);
            for (SqlNode node : with.withList) {
                SqlWithItem item = (SqlWithItem) node;
                RegisteredTable passthrough = item.columnList == null ? passthroughTable(item.query) : null;
                item.setOperand(2, query(item.query, names, parents));
                names.add(key(item.name.getSimple()));
                wildcardRelations.remove(key(item.name.getSimple()));
                if (passthrough != null) wildcardRelations.put(key(item.name.getSimple()), passthrough);
            }
            with.setOperand(1, query(with.body, names, parents));
            wildcardRelations.clear();
            wildcardRelations.putAll(previousMappings);
            translated.add(with);
            return with;
        }
        if (root instanceof SqlSelect select) return select(select, null, ctes, parents);
        if (root instanceof SqlCall call && SqlTrees.isSetOperation(root)) {
            for (int index = 0; index < call.operandCount(); index++)
                call.setOperand(index, query(call.operand(index), ctes, parents));
            return call;
        }
        throw new UnsupportedSqlException("unsupported logical query root: " + root.getKind());
    }

    private SqlNode select(SqlSelect select, SqlOrderBy order, Set<String> ctes,
                           Map<String, RegisteredTable> parents) {
        Map<String, RegisteredTable> sources = new LinkedHashMap<>();
        if (select.getFrom() != null) select.setFrom(source(select.getFrom(), ctes, parents, sources));
        Map<String, RegisteredTable> visible = new LinkedHashMap<>(parents);
        visible.putAll(sources);
        Set<String> aliases = new HashSet<>();
        for (SqlNode projection : select.getSelectList())
            if (projection.getKind() == SqlKind.AS) aliases.add(key(SqlTrees.outputName(projection)));
        Set<SqlNode> preserved = Collections.newSetFromMap(new IdentityHashMap<>());
        SqlTrees.collectAliasReferences(order == null ? select.getOrderList() : order.orderList, aliases, preserved);
        SqlTrees.collectAliasReferences(select.getGroup(), aliases, preserved);
        SqlTrees.collectAliasReferences(select.getHaving(), aliases, preserved);

        SqlShuttle resolver = new SqlShuttle() {
            @Override public SqlNode visit(SqlIdentifier id) {
                if (id.isStar() || preserved.contains(id)) return id;
                String column = id.names.get(id.names.size() - 1);
                if (id.names.size() > 1) {
                    RegisteredTable table = visible.get(key(id.names.get(id.names.size() - 2)));
                    if (table == null) return id; // derived/CTE columns keep their logical aliases
                    return table.physicalColumnFor(column).<SqlNode>map(physical -> new SqlIdentifier(
                            List.of(id.names.get(id.names.size() - 2), physical), id.getParserPosition())).orElse(id);
                }
                Set<String> physical = new LinkedHashSet<>();
                sources.values().forEach(table -> table.physicalColumnFor(column).ifPresent(physical::add));
                if (physical.size() > 1)
                    throw new UnsupportedSqlException("ambiguous logical column '" + column + "'; qualify its source");
                return physical.isEmpty() ? id : new SqlIdentifier(physical.iterator().next(), id.getParserPosition());
            }
            @Override public SqlNode visit(SqlCall call) {
                if (translated.contains(call)) return call;
                if (call instanceof SqlSelect || call instanceof SqlWith || call instanceof SqlOrderBy)
                    return query(call, ctes, visible);
                if (call.getKind() == SqlKind.AS) {
                    return replaceAliasedSource(call, call.operand(0).accept(this));
                }
                return super.visit(call);
            }
        };
        SqlNodeList projections = new SqlNodeList(select.getParserPosition());
        for (SqlNode projection : select.getSelectList()) {
            SqlNode renamed = projection.accept(resolver);
            if (projection instanceof SqlIdentifier id && !id.isStar()
                    && !CalciteSql.unparse(projection).equals(CalciteSql.unparse(renamed))) {
                renamed = SqlStdOperatorTable.AS.createCall(projection.getParserPosition(), renamed,
                        new SqlIdentifier(id.names.get(id.names.size() - 1), projection.getParserPosition()));
            }
            projections.add(renamed);
        }
        select.setSelectList(projections);
        if (select.getWhere() != null) select.setWhere(select.getWhere().accept(resolver));
        if (select.getGroup() != null) select.setGroupBy((SqlNodeList) select.getGroup().accept(resolver));
        if (select.getHaving() != null) select.setHaving(select.getHaving().accept(resolver));
        // JOIN conditions live in FROM and must use this scope's bindings, not those of an inner SELECT.
        if (select.getFrom() != null) select.setFrom(select.getFrom().accept(resolver));
        translated.add(select);
        if (order == null) {
            if (select.getOrderList() != null) select.setOrderBy((SqlNodeList) select.getOrderList().accept(resolver));
            return select;
        }
        SqlOrderBy result = new SqlOrderBy(order.getParserPosition(), select,
                (SqlNodeList) order.orderList.accept(resolver), order.offset, order.fetch);
        translated.add(result);
        return result;
    }

    private SqlNode source(SqlNode node, Set<String> ctes, Map<String, RegisteredTable> parents,
                           Map<String, RegisteredTable> sources) {
        if (node instanceof SqlJoin join) {
            join.setLeft(source(join.getLeft(), ctes, parents, sources));
            join.setRight(source(join.getRight(), ctes, parents, sources));
            return join;
        }
        SqlNode relation = node;
        SqlIdentifier alias = null;
        boolean explicitColumns = node instanceof SqlCall call && node.getKind() == SqlKind.AS && call.operandCount() > 2;
        if (node.getKind() == SqlKind.AS && node instanceof SqlCall as) {
            relation = as.operand(0);
            alias = as.operand(1);
        }
        if (relation instanceof SqlIdentifier table) {
            if (!table.isSimple()) return node; // already-physical Delta or an explicitly qualified source
            String name = table.getSimple();
            if (ctes.contains(key(name))) {
                RegisteredTable passthrough = wildcardRelations.get(key(name));
                if (passthrough != null && !explicitColumns) sources.put(key(alias == null ? name : alias.getSimple()), passthrough);
                return node;
            }
            RegisteredTable registered = catalog.findByLogicalName(name).orElseThrow(() ->
                    new UnsupportedSqlException("no registered table for logical name '" + name + "'"));
            String sentinel = sentinelPrefix + "_" + paths.size();
            paths.put(sentinel, SqlTrees.deltaSource(registered.deltaPath()));
            SqlIdentifier binding = alias == null ? table : alias;
            if (!explicitColumns) sources.put(key(binding.getSimple()), registered);
            SqlNode physical = new SqlIdentifier(sentinel, SqlParserPos.ZERO);
            return alias == null ? SqlStdOperatorTable.AS.createCall(node.getParserPosition(), physical, binding)
                    : replaceAliasedSource((SqlCall) node, physical);
        }
        RegisteredTable passthrough = passthroughTable(relation);
        SqlNode nested = query(relation, ctes, parents);
        if (passthrough != null && !explicitColumns) sources.put(key(alias == null ? "__derived" : alias.getSimple()), passthrough);
        return alias == null ? nested : replaceAliasedSource((SqlCall) node, nested);
    }

    private static SqlNode replaceAliasedSource(SqlCall call, SqlNode source) {
        SqlNode[] operands = call.getOperandList().toArray(SqlNode[]::new);
        operands[0] = source;
        return call.getOperator().createCall(call.getParserPosition(), operands);
    }

    private RegisteredTable passthroughTable(SqlNode relation) {
        SqlNode root = relation instanceof SqlOrderBy order ? order.query : relation;
        if (!(root instanceof SqlSelect select) || select.getSelectList().size() != 1
                || !(select.getSelectList().get(0) instanceof SqlIdentifier star) || !star.isStar()) return null;
        SqlNode source = SqlTrees.stripAlias(select.getFrom());
        if (source instanceof SqlIdentifier table && table.isSimple()) {
            RegisteredTable inherited = wildcardRelations.get(key(table.getSimple()));
            return inherited != null ? inherited : catalog.findByLogicalName(table.getSimple()).orElse(null);
        }
        return source == null ? null : passthroughTable(source);
    }

    private static String key(String value) { return value.toLowerCase(Locale.ROOT); }
}
