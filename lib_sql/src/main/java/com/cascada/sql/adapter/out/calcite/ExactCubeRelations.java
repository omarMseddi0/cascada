package com.cascada.sql.adapter.out.calcite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/** Finds independent aggregate relations while leaving resolution of the surrounding SQL to Spark. */
final class ExactCubeRelations {
    private static final int MAX_SELECTS = 32;
    private final SqlNode root;
    private final String sourceToken;
    private final Map<SqlSelect, Map<String, SqlWithItem>> scopes = new IdentityHashMap<>();
    private final Map<SqlWithItem, Map<String, SqlWithItem>> definitionScopes = new IdentityHashMap<>();

    ExactCubeRelations(SqlNode root, String sourceToken) {
        this.root = root;
        this.sourceToken = sourceToken;
    }

    List<SqlSelect> aggregates() {
        var candidates = new ArrayList<SqlSelect>();
        Set<SqlNode> ordered = Collections.newSetFromMap(new IdentityHashMap<>());
        root.accept(new SqlShuttle() {
            private final Set<SqlNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
            private Map<String, SqlWithItem> scope = Map.of();
            private int selects;

            @Override public SqlNode visit(SqlCall call) {
                if (call instanceof SqlWith with) {
                    var previous = scope;
                    var local = new HashMap<>(scope);
                    for (SqlNode node : with.withList) {
                        var item = (SqlWithItem) node;
                        require(!item.recursive.booleanValue());
                        definitionScopes.put(item, Map.copyOf(local));
                        local.put(item.name.getSimple(), item);
                    }
                    scope = local;
                    with.body.accept(this);
                    scope = previous;
                    return call;
                }
                if (call instanceof SqlOrderBy order) ordered.add(order.query);
                if (call instanceof SqlSelect select) {
                    if (!visited.add(select)) return call;
                    require(++selects <= MAX_SELECTS);
                    scopes.put(select, scope);
                    if (!ordered.contains(select) && select.getSelectList().stream().anyMatch(SqlTrees::containsAggregate)) {
                        candidates.add(select);
                    }
                    followReferences(select.getFrom());
                }
                return super.visit(call);
            }

            private void followReferences(SqlNode node) {
                if (node instanceof SqlJoin join) {
                    followReferences(join.getLeft());
                    followReferences(join.getRight());
                } else if (SqlTrees.stripAlias(node) instanceof SqlIdentifier id && id.isSimple()) {
                    SqlWithItem cte = scope.get(id.getSimple());
                    if (cte != null) {
                        var previous = scope;
                        scope = definitionScopes.get(cte);
                        cte.query.accept(this);
                        scope = previous;
                    }
                }
            }
        });
        return candidates;
    }

    String sourcePath(SqlSelect aggregate, java.util.function.BiConsumer<SqlNode, String> collectFilter) {
        SqlSelect select = aggregate;
        for (int depth = 0; depth < MAX_SELECTS; depth++) {
            SqlNode from = select.getFrom();
            SqlNode input = SqlTrees.stripAlias(from);
            String qualifier = null;
            if (from != input) {
                var alias = (SqlCall) from;
                require(alias.operandCount() == 2 && alias.operand(1) instanceof SqlIdentifier);
                qualifier = ((SqlIdentifier) alias.operand(1)).getSimple();
            }
            if (select.getWhere() != null) collectFilter.accept(select.getWhere(), qualifier);
            if (input instanceof SqlIdentifier table) {
                if (table.names.size() == 2 && table.names.getFirst().equalsIgnoreCase("delta")) {
                    return table.names.getLast();
                }
                require(table.isSimple());
                var cte = scopes.getOrDefault(select, Map.of()).get(table.getSimple());
                require(cte != null);
                require(cte.columnList == null);
                input = cte.query;
            }
            require(input instanceof SqlSelect && transparent((SqlSelect) input));
            select = (SqlSelect) input;
        }
        throw new IllegalArgumentException("aggregate input exceeds query graph budget");
    }

    private static boolean transparent(SqlSelect select) {
        var projection = select.getSelectList();
        return !select.hasHints() && !select.isDistinct() && !select.isKeywordPresent(SqlSelectKeyword.STREAM)
                && select.getGroup() == null && select.getHaving() == null && select.getQualify() == null
                && select.getWindowList().isEmpty() && (select.getOrderList() == null || select.getOrderList().isEmpty())
                && select.getFetch() == null && select.getOffset() == null
                && projection.size() == 1 && projection.get(0) instanceof SqlIdentifier star
                && star.isStar() && star.names.size() == 1;
    }

    String statement(String path, SqlSelect replacement, String relation) {
        SqlNode substitute = replacement == null ? null : CalciteSql.parseQuery("SELECT * FROM " + relation);
        var rewriter = new SqlShuttle() {
            private final Set<String> ctes = new java.util.HashSet<>();

            @Override public SqlNode visit(SqlCall call) {
                if (call == replacement) return substitute;
                if (call instanceof SqlWith with) {
                    var previous = Set.copyOf(ctes);
                    var items = new org.apache.calcite.sql.SqlNodeList(with.withList.getParserPosition());
                    for (SqlNode node : with.withList) {
                        var item = (SqlWithItem) node;
                        require(!item.recursive.booleanValue());
                        SqlNode bound = item.accept(this);
                        items.add(bound);
                        ctes.add(item.name.getSimple());
                    }
                    SqlNode body = with.body.accept(this);
                    ctes.retainAll(previous);
                    return new SqlWith(with.getParserPosition(), items, body);
                }
                if (call instanceof SqlSelect select) {
                    SqlSelect visited = (SqlSelect) super.visit(call);
                    SqlNode from = bind(visited.getFrom());
                    if (from == visited.getFrom()) return visited;
                    SqlSelect copy = (SqlSelect) visited.clone(visited.getParserPosition());
                    copy.setFrom(from);
                    return copy;
                }
                return super.visit(call);
            }

            private SqlNode bind(SqlNode node) {
                if (node == null || node instanceof SqlSelect || node instanceof SqlOrderBy
                        || node instanceof SqlWith || SqlTrees.isSetOperation(node) || node.getKind() == SqlKind.VALUES) {
                    return node;
                }
                if (node instanceof SqlIdentifier table) {
                    if (table.isSimple()) {
                        require(ctes.contains(table.getSimple()) || table.getSimple().equals(sourceToken));
                        return table;
                    }
                    require(table.names.size() == 2 && table.names.getFirst().equalsIgnoreCase("delta")
                            && table.names.getLast().equals(path));
                    return new SqlIdentifier(sourceToken, SqlParserPos.ZERO);
                }
                if (node instanceof SqlJoin join) {
                    var copy = (SqlJoin) join.clone(join.getParserPosition());
                    copy.setLeft(bind(join.getLeft()));
                    copy.setRight(bind(join.getRight()));
                    return copy;
                }
                if (node instanceof SqlCall alias && alias.getKind() == SqlKind.AS) {
                    SqlNode[] operands = alias.getOperandList().toArray(SqlNode[]::new);
                    operands[0] = bind(operands[0]);
                    return alias.getOperator().createCall(alias.getParserPosition(), operands);
                }
                throw new IllegalArgumentException("unbound relation in exact cube query");
            }
        };
        return CalciteSql.unparse(root.accept(rewriter));
    }

    private static void require(boolean condition) {
        if (!condition) throw new IllegalArgumentException("unbound or over-budget query graph");
    }
}
