package com.cascada.sql.canonical;

import com.cascada.cache.domain.CanonicalQueryObject;
import com.cascada.cache.domain.HashComponents;
import com.cascada.cache.domain.OrderByClause;
import com.cascada.cache.domain.PostProcessing;
import com.cascada.cache.domain.QueryMetadata;
import com.cascada.cache.domain.TimeRange;
import com.cascada.sql.calcite.CalciteSql;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Extracts a {@link CanonicalQueryObject} from a SQL string using <b>Apache Calcite</b>, porting the
 * logic the Python {@code smart_sql_processor} (sqlglot) + {@code create_canonical_object}
 * ({@code cache_component_adapter.py}) performed: pull group-by, aggregates, filters, time range,
 * ordering and limit; expand {@code AVG} via {@link AggregateNormalizer}; detect time-series and the
 * user's bucket step; and record composite aliases so they can be rebuilt after merge.
 *
 * <p>Calcite is the Java analogue of sqlglot — a full navigable {@code SqlNode} tree and a dialect
 * unparser — replacing the previous JSqlParser implementation. Following the bypass-on-unsupported
 * policy, anything Calcite cannot parse, anything that is not a simple {@code SELECT}, and anything
 * without an extractable time range raises {@link UnsupportedSqlException} so the caller bypasses to
 * Spark rather than caching a guess.
 */
public final class CalciteCanonicalObjectFactory {

    private static final Set<String> AGGREGATE_FUNCTION_NAMES = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX");

    private final AggregateNormalizer aggregateNormalizer = new AggregateNormalizer();

    public CanonicalQueryObject extractCanonicalObjectFromSql(String sql) {
        return extractCanonicalObjectFromSql(sql, TimeDimensionMap.defaults());
    }

    public CanonicalQueryObject extractCanonicalObjectFromSql(String sql, TimeDimensionMap timeDimensionMap) {
        ParsedQuery parsed = parse(sql);
        SqlSelect select = parsed.select();

        List<String> groupBy = extractGroupBy(select);
        Map<String, String> compositeAliases = new LinkedHashMap<>();
        List<String> aggregateSpecs = new ArrayList<>();
        List<String> rawAggregateExpressions = extractAggregates(select, aggregateSpecs, compositeAliases);
        List<String> projectionSignature = extractProjectionSignature(select);

        TimeRangeExtraction timeExtraction = extractTimeRangeAndFilters(select, timeDimensionMap);
        if (timeExtraction.timeRange().isEmpty()) {
            throw new UnsupportedSqlException("query has no extractable time range; bypassing cache");
        }

        TimeSeriesShape shape = detectTimeSeries(select, timeDimensionMap);

        AggregateNormalizer.NormalizedAggregates normalized = aggregateNormalizer.normalize(rawAggregateExpressions);

        HashComponents hashComponents = new HashComponents(
                new ArrayList<>(new TreeSet<>(groupBy)),
                normalized.normalizedForHash(),
                timeExtraction.filters(),
                0);

        QueryMetadata metadata = new QueryMetadata(
                shape.isTimeSeries(),
                0L,
                normalized.originalAggregates(),
                compositeAliases,
                shape.userStepSeconds(),
                shape.preserveRawTimeSeries(),
                aggregateSpecs);

        PostProcessing postProcessing = new PostProcessing(extractLimit(parsed), extractOrderBy(parsed));

        return new CanonicalQueryObject(hashComponents, timeExtraction.timeRange().orElseThrow(), postProcessing,
                metadata, sql, extractSourceSignature(select), projectionSignature);
    }

    // --- parsing ---------------------------------------------------------------------------------

    private ParsedQuery parse(String sql) {
        SqlNode node = CalciteSql.parseQuery(sql);
        if (node instanceof SqlOrderBy orderBy && orderBy.query instanceof SqlSelect select) {
            return new ParsedQuery(select, orderBy);
        }
        if (node instanceof SqlSelect select) {
            return new ParsedQuery(select, null);
        }
        throw new UnsupportedSqlException("only a simple SELECT can be canonicalised, got: "
                + node.getKind());
    }

    // --- group by --------------------------------------------------------------------------------

    private List<String> extractGroupBy(SqlSelect select) {
        List<String> groupBy = new ArrayList<>();
        SqlNodeList group = select.getGroup();
        if (group != null) {
            for (SqlNode expression : group) {
                groupBy.add(CalciteSql.unparse(expression));
            }
        }
        return groupBy;
    }

    // --- aggregates ------------------------------------------------------------------------------

    private List<String> extractAggregates(SqlSelect select, List<String> aggregateSpecsOut,
                                           Map<String, String> compositeAliasesOut) {
        List<String> rawAggregateExpressions = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            SqlNode expression = item;
            String aliasName = null;
            if (item.getKind() == SqlKind.AS) {
                SqlBasicCall asCall = (SqlBasicCall) item;
                expression = asCall.operand(0);
                aliasName = ((SqlIdentifier) asCall.operand(1)).getSimple();
            }

            List<SqlBasicCall> aggregatesInItem = new ArrayList<>();
            collectAggregateFunctions(expression, aggregatesInItem);
            if (aggregatesInItem.isEmpty()) {
                continue;
            }

            for (SqlBasicCall aggregate : aggregatesInItem) {
                rawAggregateExpressions.add(CalciteSql.unparse(aggregate));
            }

            boolean isComposite = aggregatesInItem.size() > 1 || !isSingleAggregateCall(expression);
            if (isComposite && aliasName != null) {
                compositeAliasesOut.put(aliasName, CalciteSql.unparse(expression));
            }

            String spec = aliasName != null
                    ? CalciteSql.unparse(expression) + " AS " + aliasName
                    : CalciteSql.unparse(expression);
            aggregateSpecsOut.add(spec);
        }
        return rawAggregateExpressions;
    }

    private boolean isSingleAggregateCall(SqlNode expression) {
        return expression instanceof SqlBasicCall call && isAggregateName(call);
    }

    private boolean isAggregateName(SqlBasicCall call) {
        return AGGREGATE_FUNCTION_NAMES.contains(call.getOperator().getName().toUpperCase(Locale.ROOT));
    }

    private void collectAggregateFunctions(SqlNode expression, List<SqlBasicCall> collector) {
        if (!(expression instanceof SqlCall call)) {
            return;
        }
        if (call instanceof SqlBasicCall basicCall && isAggregateName(basicCall)) {
            collector.add(basicCall);
            return;
        }
        for (SqlNode operand : call.getOperandList()) {
            if (operand != null) {
                collectAggregateFunctions(operand, collector);
            }
        }
    }

    // --- projection / source signatures ----------------------------------------------------------

    private List<String> extractProjectionSignature(SqlSelect select) {
        List<String> projections = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            projections.add(CalciteSql.unparse(item));
        }
        return new ArrayList<>(new TreeSet<>(projections));
    }

    private List<String> extractSourceSignature(SqlSelect select) {
        Set<String> sources = new TreeSet<>();
        SqlNode from = select.getFrom();
        if (from != null) {
            collectSources(from, sources);
        }
        return new ArrayList<>(sources);
    }

    private void collectSources(SqlNode from, Set<String> sources) {
        switch (from.getKind()) {
            case JOIN -> {
                org.apache.calcite.sql.SqlJoin join = (org.apache.calcite.sql.SqlJoin) from;
                collectSources(join.getLeft(), sources);
                collectSources(join.getRight(), sources);
            }
            case AS -> sources.add(CalciteSql.unparse(((SqlBasicCall) from).operand(0)));
            default -> sources.add(CalciteSql.unparse(from));
        }
    }

    // --- time range + filters --------------------------------------------------------------------

    private TimeRangeExtraction extractTimeRangeAndFilters(SqlSelect select, TimeDimensionMap timeDimensionMap) {
        List<SqlNode> conjuncts = new ArrayList<>();
        flattenAndConjuncts(select.getWhere(), conjuncts);

        Long start = null;
        Long end = null;
        List<String> filters = new ArrayList<>();

        for (SqlNode conjunct : conjuncts) {
            TimeBound bound = asTimeBound(conjunct, timeDimensionMap);
            if (bound == null) {
                filters.add(CalciteSql.unparse(conjunct));
                continue;
            }
            if (bound.start() != null) {
                start = bound.start();
            }
            if (bound.end() != null) {
                end = bound.end();
            }
        }

        Optional<TimeRange> timeRange = (start != null && end != null && end >= start)
                ? Optional.of(new TimeRange(start, end))
                : Optional.empty();
        return new TimeRangeExtraction(timeRange, new ArrayList<>(new TreeSet<>(filters)));
    }

    private void flattenAndConjuncts(SqlNode where, List<SqlNode> collector) {
        if (where == null) {
            return;
        }
        if (where.getKind() == SqlKind.AND) {
            for (SqlNode operand : ((SqlBasicCall) where).getOperandList()) {
                flattenAndConjuncts(operand, collector);
            }
        } else {
            collector.add(where);
        }
    }

    private TimeBound asTimeBound(SqlNode conjunct, TimeDimensionMap timeDimensionMap) {
        if (!(conjunct instanceof SqlBasicCall call)) {
            return null;
        }
        return switch (conjunct.getKind()) {
            case GREATER_THAN_OR_EQUAL, GREATER_THAN ->
                    boundIfTimeColumn(call.operand(0), call.operand(1), timeDimensionMap, true);
            case LESS_THAN_OR_EQUAL, LESS_THAN ->
                    boundIfTimeColumn(call.operand(0), call.operand(1), timeDimensionMap, false);
            case BETWEEN -> betweenBound(call, timeDimensionMap);
            default -> null;
        };
    }

    private TimeBound betweenBound(SqlBasicCall call, TimeDimensionMap timeDimensionMap) {
        SqlIdentifier column = null;
        List<Long> numbers = new ArrayList<>();
        for (SqlNode operand : call.getOperandList()) {
            if (operand instanceof SqlIdentifier identifier && column == null) {
                column = identifier;
            } else {
                Long value = asLong(operand);
                if (value != null) {
                    numbers.add(value);
                }
            }
        }
        if (column != null && isTimeColumn(column, timeDimensionMap) && numbers.size() == 2) {
            return new TimeBound(numbers.get(0), numbers.get(1));
        }
        return null;
    }

    private TimeBound boundIfTimeColumn(SqlNode left, SqlNode right, TimeDimensionMap timeDimensionMap,
                                        boolean isStart) {
        if (!(left instanceof SqlIdentifier identifier) || !isTimeColumn(identifier, timeDimensionMap)) {
            return null;
        }
        Long value = asLong(right);
        if (value == null) {
            return null;
        }
        return isStart ? new TimeBound(value, null) : new TimeBound(null, value);
    }

    private boolean isTimeColumn(SqlIdentifier identifier, TimeDimensionMap timeDimensionMap) {
        return timeDimensionMap.isTimeColumn(simpleName(identifier));
    }

    private String simpleName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1);
    }

    private Long asLong(SqlNode node) {
        if (node instanceof SqlNumericLiteral literal && literal.isInteger()) {
            return literal.getValueAs(Long.class);
        }
        if (node instanceof SqlLiteral literal && literal.getTypeName().getFamily() != null) {
            try {
                return literal.getValueAs(Long.class);
            } catch (RuntimeException notLong) {
                return null;
            }
        }
        return null;
    }

    // --- time-series detection -------------------------------------------------------------------

    private TimeSeriesShape detectTimeSeries(SqlSelect select, TimeDimensionMap timeDimensionMap) {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return new TimeSeriesShape(false, Optional.empty(), false);
        }
        for (SqlNode expression : group) {
            Optional<Integer> step = floorBucketStep(expression, timeDimensionMap);
            if (step.isPresent()) {
                return new TimeSeriesShape(true, step, false);
            }
        }
        for (SqlNode expression : group) {
            if (expression instanceof SqlIdentifier identifier && isTimeColumn(identifier, timeDimensionMap)) {
                // Grouping directly by a raw time column: a time series with no synthetic step.
                return new TimeSeriesShape(true, Optional.empty(), true);
            }
        }
        return new TimeSeriesShape(false, Optional.empty(), false);
    }

    /**
     * Detect {@code FLOOR(timeCol / N) * N} (in either factor order, possibly wrapped in CAST) and
     * return {@code N}. Mirrors the Python regex {@code FLOOR(col / N) * N} bucket recognition.
     */
    private Optional<Integer> floorBucketStep(SqlNode expression, TimeDimensionMap timeDimensionMap) {
        SqlNode node = unwrapCast(expression);
        if (node.getKind() != SqlKind.TIMES || !(node instanceof SqlBasicCall times)) {
            return Optional.empty();
        }
        SqlNode left = times.operand(0);
        SqlNode right = times.operand(1);
        SqlNode floor = left.getKind() == SqlKind.FLOOR ? left : right.getKind() == SqlKind.FLOOR ? right : null;
        if (floor == null) {
            return Optional.empty();
        }
        SqlNode divide = ((SqlBasicCall) floor).operand(0);
        if (divide.getKind() != SqlKind.DIVIDE) {
            return Optional.empty();
        }
        SqlBasicCall division = (SqlBasicCall) divide;
        if (!(division.operand(0) instanceof SqlIdentifier column) || !isTimeColumn(column, timeDimensionMap)) {
            return Optional.empty();
        }
        Long step = asLong(division.operand(1));
        return step == null ? Optional.empty() : Optional.of(step.intValue());
    }

    private SqlNode unwrapCast(SqlNode node) {
        if (node.getKind() == SqlKind.CAST && node instanceof SqlBasicCall cast) {
            return cast.operand(0);
        }
        return node;
    }

    // --- order by / limit ------------------------------------------------------------------------

    private Optional<Integer> extractLimit(ParsedQuery parsed) {
        SqlNode fetch = parsed.fetch();
        Long value = fetch == null ? null : asLong(fetch);
        return value == null ? Optional.empty() : Optional.of(value.intValue());
    }

    private List<OrderByClause> extractOrderBy(ParsedQuery parsed) {
        List<OrderByClause> orderBy = new ArrayList<>();
        SqlNodeList orderList = parsed.orderList();
        if (orderList == null) {
            return orderBy;
        }
        for (SqlNode element : orderList) {
            boolean ascending = true;
            boolean nullsFirst = false;
            SqlNode current = element;
            boolean unwrapping = true;
            while (unwrapping) {
                switch (current.getKind()) {
                    case DESCENDING -> {
                        ascending = false;
                        current = ((SqlBasicCall) current).operand(0);
                    }
                    case NULLS_FIRST -> {
                        nullsFirst = true;
                        current = ((SqlBasicCall) current).operand(0);
                    }
                    case NULLS_LAST -> {
                        nullsFirst = false;
                        current = ((SqlBasicCall) current).operand(0);
                    }
                    default -> unwrapping = false;
                }
            }
            if (current instanceof SqlIdentifier identifier) {
                orderBy.add(OrderByClause.forColumn(simpleName(identifier), ascending, nullsFirst));
            } else {
                orderBy.add(OrderByClause.forExpression(CalciteSql.unparse(current), ascending, nullsFirst));
            }
        }
        return orderBy;
    }

    // --- carriers --------------------------------------------------------------------------------

    private record ParsedQuery(SqlSelect select, SqlOrderBy orderBy) {
        SqlNode fetch() {
            return orderBy == null ? select.getFetch() : orderBy.fetch;
        }

        SqlNodeList orderList() {
            if (orderBy != null) {
                return orderBy.orderList;
            }
            return select.getOrderList();
        }
    }

    private record TimeRangeExtraction(Optional<TimeRange> timeRange, List<String> filters) {
    }

    private record TimeBound(Long start, Long end) {
    }

    private record TimeSeriesShape(boolean isTimeSeries, Optional<Integer> userStepSeconds,
                                   boolean preserveRawTimeSeries) {
    }
}
