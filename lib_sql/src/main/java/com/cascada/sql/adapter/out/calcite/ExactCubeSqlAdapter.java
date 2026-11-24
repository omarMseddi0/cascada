package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.application.port.out.ExactCubeSqlPort;
import com.cascada.cache.domain.model.cube.ExactCubeQuery;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeKeyFilter;
import com.cascada.cache.domain.model.cube.CubeKeyFilterOperator;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubeOperation;
import com.cascada.cache.domain.model.cube.CubeProjection;
import com.cascada.cache.domain.model.cube.CubeSort;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.type.SqlTypeName;

/** Proves one aggregate over a registered relation, optionally directly below an outer SELECT. */
public final class ExactCubeSqlAdapter implements ExactCubeSqlPort {
    private static final Set<SqlKind> PREDICATES = EnumSet.of(SqlKind.AND, SqlKind.OR, SqlKind.NOT,
            SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL, SqlKind.IS_NULL, SqlKind.IS_NOT_NULL);
    private final Set<String> paths;
    private final String timeColumn;

    public ExactCubeSqlAdapter(Set<String> paths, String timeColumn) {
        this.paths = Set.copyOf(paths);
        this.timeColumn = timeColumn;
    }

    @Override
    public Optional<ExactCubeQuery> recognize(String physicalSql) {
        if (physicalSql.contains("${")) return Optional.empty();
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
    public String pinnedBucketStatement(String sql, ExactCubeSource source) {
        String token = SqlTrees.unusedSentinel(sql, "CASCADA_BUCKET_PIN");
        return new ExactCubeRelations(CalciteSql.parseQuery(sql), token).statement(source.path(), null, null)
                .replace(token, ExactCubeSql.relation(source));
    }

    @Override
    public String outerStatement(ExactCubeQuery query, ExactCubeSource source) {
        return ExactCubeSql.outer(query, source);
    }

    private ExactCubeQuery extract(String physicalSql) {
        SqlNode root = CalciteSql.parseQuery(physicalSql);
        SqlNode body = root instanceof SqlOrderBy order ? order.query : root;
        String sourceToken = SqlTrees.unusedSentinel(physicalSql, "CASCADA_PINNED_SOURCE");
        String outerToken = SqlTrees.unusedSentinel(physicalSql, "CASCADA_COMPLETE_AGGREGATE");

        var relations = new ExactCubeRelations(root, sourceToken);
        // Try the top-level aggregate first so ordinary hits need no graph traversal.
        if (body instanceof SqlSelect select) {
            var direct = candidate(root, relations, select, true, sourceToken, outerToken);
            if (direct != null) return direct;
        }
        for (SqlSelect select : relations.aggregates()) {
            var nested = candidate(root, relations, select, select == body, sourceToken, outerToken);
            if (nested != null) return nested;
        }
        throw new IllegalArgumentException("no independent aggregate relation");
    }

    private ExactCubeQuery candidate(SqlNode root, ExactCubeRelations relations, SqlSelect inner, boolean direct,
                                     String sourceToken, String outerToken) {
        try {
            return aggregate(root, relations, inner, direct, sourceToken, outerToken);
        } catch (IllegalArgumentException | ArithmeticException | UnsupportedSqlException ineligible) {
            return null;
        }
    }

    private ExactCubeQuery aggregate(SqlNode root, ExactCubeRelations relations, SqlSelect inner, boolean direct,
                                     String sourceToken, String outerToken) {
        require(coreSelect(inner) && !inner.isDistinct() && inner.getHaving() == null);
        SqlNode from = inner.getFrom();
        SqlNode unaliased = SqlTrees.stripAlias(from);
        require(from != null && (from == unaliased || ((SqlCall) from).operandCount() == 2));
        String qualifier = from == unaliased ? null : simple(((SqlCall) from).operand(1));
        var conjuncts = new ArrayList<SqlNode>();
        String path = relations.sourcePath(inner, (filter, filterQualifier) -> {
            var layer = new ArrayList<SqlNode>();
            flatten(filter, layer);
            var binder = new org.apache.calcite.sql.util.SqlShuttle() {
                @Override public SqlNode visit(SqlIdentifier id) {
                    return new SqlIdentifier(column(id, filterQualifier), id.getParserPosition());
                }
            };
            for (SqlNode term : layer) {
                require(predicate(term));
                conjuncts.add(term.accept(binder));
            }
        });
        require(paths.contains(path));
        var predicates = new TreeSet<String>();
        var keyFilters = new HashSet<CubeKeyFilter>();
        boolean allKeyFilters = true;
        var predicateTypes = new HashMap<String, String>();
        Long lower = null;
        Long upper = null;
        for (SqlNode node : conjuncts) {
            if (node instanceof SqlCall call && call.operandCount() == 2
                    && call.operand(0) instanceof SqlIdentifier id && column(id, qualifier).equals(timeColumn)) {
                if (call.getKind() == SqlKind.GREATER_THAN_OR_EQUAL || call.getKind() == SqlKind.GREATER_THAN) {
                    long value = number(call.operand(1));
                    long start = call.getKind() == SqlKind.GREATER_THAN ? Math.incrementExact(value) : value;
                    lower = lower == null ? start : Math.max(lower, start);
                    continue;
                }
                if (call.getKind() == SqlKind.LESS_THAN || call.getKind() == SqlKind.LESS_THAN_OR_EQUAL) {
                    long value = number(call.operand(1));
                    long end = call.getKind() == SqlKind.LESS_THAN_OR_EQUAL ? Math.incrementExact(value) : value;
                    upper = upper == null ? end : Math.min(upper, end);
                    continue;
                }
            }
            predicateTypes(node, predicateTypes);
            predicates.add(CalciteSql.unparse(node));
            var keyFilter = keyFilter(node);
            if (keyFilter == null) allKeyFilters = false;
            else keyFilters.add(keyFilter);
        }
        require(lower != null && upper != null);
        var projectedKeys = new HashMap<String, String>();
        for (SqlNode item : inner.getSelectList()) {
            if (item.getKind() == SqlKind.AS && SqlTrees.unaliased(item) instanceof SqlIdentifier id) {
                String alias = SqlTrees.outputName(item);
                String original = column(id, qualifier);
                if (!alias.equals(original)) projectedKeys.put(alias, original);
            }
        }
        var groupAliases = new HashSet<String>();
        var grain = new ArrayList<String>();
        if (inner.getGroup() != null) {
            for (SqlNode key : inner.getGroup()) {
                String name = column(key, qualifier);
                if (((SqlIdentifier) key).isSimple() && projectedKeys.containsKey(name)) {
                    groupAliases.add(name);
                    name = projectedKeys.get(name);
                }
                require(!name.equals(timeColumn) && !grain.contains(name));
                grain.add(name);
            }
        }
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
                column = column(expression, qualifier);
                require(grain.contains(column));
                operation = CubeOperation.KEY;
            } else {
                require(expression instanceof SqlCall && item != expression);
                SqlCall aggregate = (SqlCall) expression;
                require(aggregate.getFunctionQuantifier() == null && aggregate.operandCount() == 1);
                operation = CubeOperation.valueOf(aggregate.getOperator().getName().toUpperCase(Locale.ROOT));
                require(operation != CubeOperation.KEY);
                column = column(aggregate.operand(0), qualifier);
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
        if (direct && root instanceof SqlOrderBy ordered) {
            require(ordered.offset == null);
            fetch = ordered.fetch;
            sort = ordered.orderList;
        }
        List<CubeSort> order = sorts(sort, projections);
        Integer limit = fetch == null ? null : Math.toIntExact(number(fetch));
        require(limit == null || limit >= 0 && !order.isEmpty());
        require(direct || (order.isEmpty() && limit == null));

        String pinned = relations.statement(path, null, null);
        String outerSql = direct ? null : relations.statement(path, inner, outerToken);
        return new ExactCubeQuery(path, timeColumn, new CubeInterval(lower, upper),
                String.join(" AND ", predicates.stream().map(p -> "(" + p + ")").toList()),
                predicateTypes, allKeyFilters ? List.copyOf(keyFilters) : List.of(),
                grain, groupAliases, input, measures, projections, order, limit, outerSql, outerToken, pinned, sourceToken);
    }

    private static boolean coreSelect(SqlSelect select) {
        return !select.hasHints() && !select.isKeywordPresent(SqlSelectKeyword.STREAM)
                && select.getQualify() == null && select.getWindowList().isEmpty();
    }

    private static void flatten(SqlNode node, List<SqlNode> out) {
        require(node != null);
        if (node instanceof SqlCall call && call.getKind() == SqlKind.AND) {
            call.getOperandList().forEach(child -> flatten(child, out));
        } else {
            out.add(node);
        }
    }

    private static void predicateTypes(SqlNode node, Map<String, String> types) {
        if (node instanceof SqlIdentifier id) {
            requireType(types, id, "boolean");
            return;
        }
        if (node instanceof SqlLiteral literal) {
            require(literal.getTypeName() == SqlTypeName.BOOLEAN || literal.getTypeName() == SqlTypeName.NULL);
            return;
        }
        var call = (SqlCall) node;
        switch (call.getKind()) {
            case AND, OR, NOT -> call.getOperandList().forEach(operand -> predicateTypes(operand, types));
            case IS_NULL, IS_NOT_NULL -> {
                require(call.operand(0) instanceof SqlIdentifier);
                requireType(types, call.operand(0), "");
            }
            default -> {
                SqlNode first = call.operand(0);
                SqlNode second = call.operand(1);
                if (second instanceof SqlIdentifier) {
                    SqlNode temporary = first;
                    first = second;
                    second = temporary;
                }
                require(first instanceof SqlIdentifier && second instanceof SqlLiteral);
                var literal = (SqlLiteral) second;
                String required = switch (literal.getTypeName()) {
                    case NULL -> "";
                    case CHAR -> "string";
                    case BOOLEAN -> "boolean";
                    case DECIMAL -> {
                        number(second); // integer literal representable without narrowing or rounding
                        yield "integral";
                    }
                    default -> throw new IllegalArgumentException("unsupported typed predicate literal");
                };
                requireType(types, (SqlIdentifier) first, required);
            }
        }
    }

    private static void requireType(Map<String, String> types, SqlIdentifier id, String required) {
        types.merge(id.getSimple(), required, (previous, next) -> {
            if (previous.isEmpty()) return next;
            require(next.isEmpty() || previous.equals(next));
            return previous;
        });
    }

    private static CubeKeyFilter keyFilter(SqlNode node) {
        if (!(node instanceof SqlCall call)) return null;
        SqlNode left = call.operand(0);
        if (call.getKind() == SqlKind.IS_NULL || call.getKind() == SqlKind.IS_NOT_NULL) {
            if (!(left instanceof SqlIdentifier column)) return null;
            var operator = call.getKind() == SqlKind.IS_NULL ? CubeKeyFilterOperator.IS_NULL : CubeKeyFilterOperator.IS_NOT_NULL;
            return new CubeKeyFilter(column.getSimple(), operator, null);
        }
        if (call.operandCount() != 2) return null;
        SqlNode right = call.operand(1);
        boolean reversed = right instanceof SqlIdentifier;
        if (!((reversed ? right : left) instanceof SqlIdentifier column)) return null;
        SqlNode value = reversed ? left : right;
        if (call.getKind() == SqlKind.EQUALS && value instanceof SqlCharStringLiteral literal
                && literal.getNlsString().getCharsetName() == null
                && literal.getNlsString().getCollation() == null
                && literal.getNlsString().getValue().matches("[A-Za-z0-9 _.:-]*")) {
            return new CubeKeyFilter(column.getSimple(), CubeKeyFilterOperator.STRING_EQUALS,
                    literal.getNlsString().getValue());
        }
        if (!(value instanceof SqlNumericLiteral literal) || !literal.isInteger()) return null;
        var operator = switch (call.getKind()) {
            case EQUALS -> CubeKeyFilterOperator.INTEGER_EQUALS;
            case NOT_EQUALS -> CubeKeyFilterOperator.INTEGER_NOT_EQUALS;
            case LESS_THAN -> reversed ? CubeKeyFilterOperator.INTEGER_GREATER : CubeKeyFilterOperator.INTEGER_LESS;
            case LESS_THAN_OR_EQUAL -> reversed ? CubeKeyFilterOperator.INTEGER_GREATER_OR_EQUAL
                    : CubeKeyFilterOperator.INTEGER_LESS_OR_EQUAL;
            case GREATER_THAN -> reversed ? CubeKeyFilterOperator.INTEGER_LESS : CubeKeyFilterOperator.INTEGER_GREATER;
            case GREATER_THAN_OR_EQUAL -> reversed ? CubeKeyFilterOperator.INTEGER_LESS_OR_EQUAL
                    : CubeKeyFilterOperator.INTEGER_GREATER_OR_EQUAL;
            default -> null;
        };
        return operator == null ? null
                : new CubeKeyFilter(column.getSimple(), operator, Long.toString(number(value)));
    }

    private static boolean predicate(SqlNode node) {
        if (node instanceof SqlIdentifier id) return !id.isStar();
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

    private static String column(SqlNode node, String qualifier) {
        require(node instanceof SqlIdentifier);
        var id = (SqlIdentifier) node;
        if (id.isSimple()) return simple(id);
        require(id.names.size() == 2 && qualifier != null && qualifier.equals(id.names.getFirst()));
        return simple(new SqlIdentifier(id.names.getLast(), id.getParserPosition()));
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
