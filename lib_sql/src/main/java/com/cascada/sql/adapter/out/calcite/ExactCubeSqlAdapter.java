package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.application.port.out.ExactCubeSqlPort;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubeOperation;
import com.cascada.cache.domain.model.cube.CubeProjection;
import com.cascada.cache.domain.model.cube.CubeSort;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.parser.SqlParserPos;

/** Proves one aggregate over a registered relation, optionally directly below an outer SELECT. */
public final class ExactCubeSqlAdapter implements ExactCubeSqlPort {
    private static final Set<SqlKind> PREDICATES = EnumSet.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT,
            SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.IS_NULL, SqlKind.IS_NOT_NULL);
    private static final Set<SqlKind> OUTER_OPERATORS = EnumSet.of(SqlKind.AS, SqlKind.DESCENDING,
            SqlKind.NULLS_FIRST, SqlKind.NULLS_LAST, SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES,
            SqlKind.DIVIDE, SqlKind.SUM, SqlKind.COUNT, SqlKind.AVG, SqlKind.MIN, SqlKind.MAX,
            SqlKind.CASE, SqlKind.COALESCE);
    private static final Set<String> OUTER_FUNCTIONS = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX", "COALESCE");
    private final Set<String> paths;
    private final String timeColumn;

    public ExactCubeSqlAdapter(Set<String> paths, String timeColumn) {
        this.paths = Set.copyOf(paths);
        this.timeColumn = timeColumn;
    }

    @Override
    public Optional<ExactCubeQuery> recognize(String physicalSql) {
        if (physicalSql.length() > 16_384 || physicalSql.contains("${")) return Optional.empty();
        try {
            return Optional.of(extract(physicalSql));
        } catch (IllegalArgumentException | ArithmeticException | UnsupportedSqlException ineligible) {
            return Optional.empty();
        }
    }

    @Override
    public String pinnedStatement(ExactCubeQuery query, ExactCubeSource source) {
        return ExactCubeSql.direct(query, source);
    }

    @Override
    public String stateStatement(ExactCubeQuery query, ExactCubeSource source, CubeInterval range, Set<CubeMeasure> measures) {
        return ExactCubeSql.state(query, source, range, measures);
    }

    @Override
    public String outerStatement(ExactCubeQuery query, ExactCubeSource source, ResultFrame aggregate) {
        return ExactCubeSql.outer(query, source, aggregate);
    }

    private ExactCubeQuery extract(String physicalSql) {
        SqlNode root = CalciteSql.parseQuery(physicalSql);
        SqlSelect outer = select(root);
        SqlSelect inner = outer;
        SqlCall derived = null;
        if (outer.getFrom() instanceof SqlCall alias && alias.getKind() == SqlKind.AS
                && alias.operandCount() == 2 && alias.operand(0) instanceof SqlSelect subquery) {
            require(plainOuter(outer, root));
            derived = alias;
            inner = subquery;
        }
        require(coreSelect(inner) && !inner.isDistinct() && inner.getHaving() == null);
        SqlNode source = SqlTrees.stripAlias(inner.getFrom());
        require(source instanceof SqlIdentifier);
        require(inner.getFrom() == source || ((SqlCall) inner.getFrom()).operandCount() == 2);
        SqlIdentifier from = (SqlIdentifier) source;
        require(from.names.size() == 2 && from.names.getFirst().equalsIgnoreCase("delta")
                && paths.contains(from.names.getLast()));
        var predicates = new TreeSet<String>();
        Long lower = null;
        Long upper = null;
        var conjuncts = new ArrayList<SqlNode>();
        flatten(inner.getWhere(), conjuncts);
        for (SqlNode node : conjuncts) {
            if (node instanceof SqlCall call && call.operandCount() == 2
                    && call.operand(0) instanceof SqlIdentifier id && id.isSimple()
                    && id.getSimple().equals(timeColumn)) {
                if (call.getKind() == SqlKind.GREATER_THAN_OR_EQUAL || call.getKind() == SqlKind.GREATER_THAN) {
                    require(lower == null);
                    long value = number(call.operand(1));
                    lower = call.getKind() == SqlKind.GREATER_THAN ? Math.incrementExact(value) : value;
                    continue;
                }
                if (call.getKind() == SqlKind.LESS_THAN || call.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
                    require(upper == null);
                    long value = number(call.operand(1));
                    upper = call.getKind() == SqlKind.LESS_THAN_OR_EQUAL ? Math.incrementExact(value) : value;
                    continue;
                }
            }
            require(predicate(node));
            predicates.add(CalciteSql.unparse(node));
        }
        require(lower != null && upper != null);
        var grain = new ArrayList<String>();
        if (inner.getGroup() != null) {
            for (SqlNode key : inner.getGroup()) {
                String name = simple(key);
                require(!name.equals(timeColumn) && !grain.contains(name));
                grain.add(name);
            }
        }
        require(grain.size() <= 4 && inner.getSelectList().size() <= 8);
        var projections = new ArrayList<CubeProjection>();
        var labels = new HashSet<String>();
        var measures = EnumSet.noneOf(CubeMeasure.class);
        String input = null;
        for (SqlNode item : inner.getSelectList()) {
            SqlNode expression = SqlTrees.unaliased(item);
            String label = SqlTrees.outputName(item);
            CubeOperation operation;
            String column;
            if (expression instanceof SqlIdentifier) {
                column = simple(expression);
                require(grain.contains(column));
                operation = CubeOperation.KEY;
            } else {
                require(expression instanceof SqlCall && item != expression);
                SqlCall aggregate = (SqlCall) expression;
                require(aggregate.getFunctionQuantifier() == null && aggregate.operandCount() == 1);
                operation = CubeOperation.valueOf(aggregate.getOperator().getName().toUpperCase(Locale.ROOT));
                require(operation != CubeOperation.KEY);
                column = simple(aggregate.operand(0));
                require(input == null || input.equals(column));
                input = column;
                if (operation != CubeOperation.COUNT) measures.add(CubeMeasure.SUM);
                if (operation != CubeOperation.SUM) measures.add(CubeMeasure.COUNT);
            }
            require(label.matches("[A-Za-z_][A-Za-z_0-9]*") && labels.add(label.toLowerCase(Locale.ROOT)));
            projections.add(new CubeProjection(operation, column, label));
        }
        require(input != null && !input.equals(timeColumn));
        SqlNodeList sort = inner.getOrderList();
        SqlNode fetch = inner.getFetch();
        require(inner.getOffset() == null);
        if (derived == null && root instanceof SqlOrderBy ordered) {
            require(ordered.offset == null);
            fetch = ordered.fetch;
            sort = ordered.orderList;
        }
        List<CubeSort> order = sorts(sort, projections);
        Integer limit = fetch == null ? null : Math.toIntExact(number(fetch));
        require(limit == null || limit >= 0 && !order.isEmpty());
        require(derived == null || (order.isEmpty() && limit == null));

        String sourceToken = SqlTrees.unusedSentinel(physicalSql, "CASCADA_PINNED_SOURCE");
        String outerToken = SqlTrees.unusedSentinel(physicalSql, "CASCADA_COMPLETE_AGGREGATE");
        var sourcePlaceholder = new SqlIdentifier(sourceToken, SqlParserPos.ZERO);
        if (inner.getFrom() == source) {
            inner.setFrom(sourcePlaceholder);
        } else {
            ((SqlCall) inner.getFrom()).setOperand(0, sourcePlaceholder);
        }
        String pinned = CalciteSql.unparse(root);
        String outerSql = null;
        if (derived != null) {
            derived.setOperand(0, new SqlIdentifier(outerToken, SqlParserPos.ZERO));
            outerSql = CalciteSql.unparse(root);
        }
        return new ExactCubeQuery(from.names.getLast(), timeColumn, new CubeInterval(lower, upper),
                String.join(" AND ", predicates.stream().map(p -> "(" + p + ")").toList()),
                grain, input, measures, projections, order, limit, outerSql, outerToken, pinned, sourceToken);
    }

    private static SqlSelect select(SqlNode root) {
        SqlNode body = root instanceof SqlOrderBy order ? order.query : root;
        require(body instanceof SqlSelect);
        return (SqlSelect) body;
    }

    private static boolean coreSelect(SqlSelect select) {
        return !select.hasHints() && !select.isKeywordPresent(SqlSelectKeyword.STREAM)
                && select.getQualify() == null && select.getWindowList().isEmpty();
    }

    private static boolean plainOuter(SqlSelect select, SqlNode root) {
        return coreSelect(select) && safeOuter(select.getSelectList()) && safeOuter(select.getWhere())
                && safeOuter(select.getGroup()) && safeOuter(select.getHaving())
                && safeOuter(select.getOrderList())
                && (!(root instanceof SqlOrderBy order) || safeOuter(order.orderList));
    }

    private static boolean safeOuter(SqlNode node) {
        if (node == null || node instanceof SqlLiteral || node instanceof SqlIdentifier) return true;
        if (node instanceof SqlNodeList list) return list.stream().allMatch(ExactCubeSqlAdapter::safeOuter);
        return node instanceof SqlCall call
                && (PREDICATES.contains(call.getKind()) || OUTER_OPERATORS.contains(call.getKind())
                    || call.getOperator() instanceof SqlFunction function && function.getNameAsId().isSimple()
                        && OUTER_FUNCTIONS.contains(function.getName().toUpperCase(Locale.ROOT)))
                && call.getOperandList().stream().allMatch(ExactCubeSqlAdapter::safeOuter);
    }

    private static void flatten(SqlNode node, List<SqlNode> out) {
        require(node != null);
        if (node instanceof SqlCall call && call.getKind() == SqlKind.AND) {
            call.getOperandList().forEach(child -> flatten(child, out));
        } else {
            out.add(node);
        }
    }

    private static boolean predicate(SqlNode node) {
        if (node instanceof SqlIdentifier id) return id.isSimple() && !id.isStar();
        if (node instanceof SqlLiteral) return true;
        return node instanceof SqlCall call && PREDICATES.contains(call.getKind())
                && call.getOperandList().stream().allMatch(ExactCubeSqlAdapter::predicate);
    }

    private static List<CubeSort> sorts(SqlNodeList nodes, List<CubeProjection> projections) {
        var result = new ArrayList<CubeSort>();
        if (nodes == null) return result;
        for (SqlNode node : nodes) {
            Boolean nullsFirst = null;
            if (node.getKind() == SqlKind.NULLS_FIRST || node.getKind() == SqlKind.NULLS_LAST) {
                nullsFirst = node.getKind() == SqlKind.NULLS_FIRST;
                node = ((SqlCall) node).operand(0);
            }
            boolean descending = node.getKind() == SqlKind.DESCENDING;
            if (descending) node = ((SqlCall) node).operand(0);
            String label = simple(node);
            require(projections.stream().anyMatch(p -> p.label().equals(label)));
            result.add(new CubeSort(label, descending, nullsFirst == null ? !descending : nullsFirst));
        }
        return result;
    }

    private static String simple(SqlNode node) {
        require(node instanceof SqlIdentifier && ((SqlIdentifier) node).isSimple());
        String name = ((SqlIdentifier) node).getSimple();
        require(name.matches("[A-Za-z_][A-Za-z_0-9]*") && !name.equalsIgnoreCase("ALL"));
        return name;
    }

    private static long number(SqlNode node) {
        require(node instanceof SqlNumericLiteral && ((SqlNumericLiteral) node).isInteger());
        return ((SqlNumericLiteral) node).bigDecimalValue().longValueExact();
    }

    private static void require(boolean condition) {
        if (!condition) throw new IllegalArgumentException("unsupported exact aggregate SQL");
    }
}
