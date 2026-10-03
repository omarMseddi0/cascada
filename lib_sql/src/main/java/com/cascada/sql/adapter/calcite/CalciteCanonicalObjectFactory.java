package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.domain.UnsupportedSqlException;
import com.cascada.cache.domain.query.CanonicalQueryObject;
import com.cascada.cache.domain.hashing.HashComponents;
import com.cascada.cache.domain.query.OrderByClause;
import com.cascada.cache.domain.query.PostProcessing;
import com.cascada.cache.domain.query.QueryMetadata;
import com.cascada.cache.domain.merge.AggregateFunction;
import com.cascada.cache.application.port.out.SqlCanonicalizerPort;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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
public final class CalciteCanonicalObjectFactory implements SqlCanonicalizerPort {

    /**
     * The {@link SqlCanonicalizerPort} entry point — this is the seam the cache actually depends on.
     * It delegates to {@link #extractCanonicalObjectFromSql(String)} with the default time-dimension
     * map; use the two-argument overload directly when a domain supplies its own time column names.
     */
    @Override
    public CanonicalQueryObject canonicalize(String physicalSql) {
        return extractCanonicalObjectFromSql(physicalSql, configuredTimeDimensions);
    }

    private final TimeDimensionMap configuredTimeDimensions;

    public CalciteCanonicalObjectFactory() { this(TimeDimensionMap.defaults()); }

    public CalciteCanonicalObjectFactory(TimeDimensionMap timeDimensions) {
        this.configuredTimeDimensions = java.util.Objects.requireNonNull(timeDimensions);
    }

    private static final Set<String> AGGREGATE_FUNCTION_NAMES = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX");

    private final AggregateNormalizer aggregateNormalizer = new AggregateNormalizer();
    private final SqlCacheabilityGuard cacheabilityGuard = new SqlCacheabilityGuard();
    private final CanonicalTimeSeriesAnalyzer timeSeriesAnalyzer = new CanonicalTimeSeriesAnalyzer();

    public CanonicalQueryObject extractCanonicalObjectFromSql(String sql) {
        return extractCanonicalObjectFromSql(sql, TimeDimensionMap.defaults());
    }

    public CanonicalQueryObject extractCanonicalObjectFromSql(String sql, TimeDimensionMap timeDimensionMap) {
        ParsedSqlQuery parsed = parse(sql);
        SqlSelect select = parsed.select();
        cacheabilityGuard.validate(parsed.root(), select, timeDimensionMap);

        List<String> groupBy = extractGroupBy(select);
        Map<String, String> compositeAliases = new LinkedHashMap<>();
        List<String> aggregateSpecs = new ArrayList<>();
        Map<String, AggregateFunction> measureAggregates = new LinkedHashMap<>();
        List<SqlBasicCall> rawAggregateCalls =
                extractAggregates(select, aggregateSpecs, compositeAliases, measureAggregates);
        List<String> projectionSignature = extractProjectionSignature(select);
        List<String> logicSignature = extractLogicSignature(select);
        if (!hasMergeableProjection(parsed, timeDimensionMap)) {
            logicSignature.add("UNSUPPORTED_MERGE_SHAPE");
        }

        CanonicalTimeRangeExtraction timeExtraction =
                timeSeriesAnalyzer.extractTimeRangeAndFilters(select, timeDimensionMap);
        if (timeExtraction.timeRange().isEmpty()) {
            throw new UnsupportedSqlException("query has no extractable time range; bypassing cache");
        }

        CanonicalTimeSeriesShape shape = timeSeriesAnalyzer.detectTimeSeries(select, timeDimensionMap);

        NormalizedAggregates normalized = aggregateNormalizer.normalize(rawAggregateCalls);

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
                aggregateSpecs,
                measureAggregates);

        PostProcessing postProcessing = new PostProcessing(extractLimit(parsed), extractOrderBy(parsed));

        return new CanonicalQueryObject(hashComponents, timeExtraction.timeRange().orElseThrow(), postProcessing,
                metadata, sql, extractSourceSignature(select), projectionSignature, logicSignature);
    }

    private boolean hasMergeableProjection(ParsedSqlQuery parsed, TimeDimensionMap dimensions) {
        SqlSelect select = parsed.select();
        if ((parsed.orderBy() != null && parsed.orderBy().offset != null) || select.getOffset() != null) return false;
        if (extractOrderBy(parsed).stream().anyMatch(order -> order.expression().isPresent())) return false;
        List<String> grouped = extractGroupBy(select);
        Set<String> projectedGroups = new java.util.HashSet<>();
        for (SqlNode item : select.getSelectList()) {
            SqlNode expression = item;
            String alias = null;
            if (item.getKind() == SqlKind.AS && item instanceof SqlBasicCall as) {
                expression = as.operand(0);
                alias = ((SqlIdentifier) as.operand(1)).getSimple();
            }
            if (expression instanceof SqlBasicCall call && isAggregateName(call)) {
                if (combineFunctionFor(call.getOperator().getName()) == null || call.getFunctionQuantifier() != null) return false;
                continue;
            }
            String rendered = CalciteSql.unparse(expression);
            if (!grouped.contains(rendered)) return false;
            if (expression instanceof SqlIdentifier id && id.isSimple()
                    && (alias == null || alias.equals(id.getSimple()))) {
                projectedGroups.add(rendered);
            } else if (CanonicalTimeSeriesAnalyzer.floorBucketStep(expression, dimensions).isPresent()
                    && alias != null && dimensions.isTimeColumn(alias)) {
                projectedGroups.add(rendered);
            } else return false;
        }
        return projectedGroups.containsAll(grouped);
    }

    // --- parsing ---------------------------------------------------------------------------------

    private ParsedSqlQuery parse(String sql) {
        SqlNode node = CalciteSql.parseQuery(sql);
        if (node instanceof SqlOrderBy orderBy && orderBy.query instanceof SqlSelect select) {
            return new ParsedSqlQuery(select, orderBy);
        }
        if (node instanceof SqlSelect select) {
            return new ParsedSqlQuery(select, null);
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

    private List<SqlBasicCall> extractAggregates(SqlSelect select, List<String> aggregateSpecsOut,
                                                 Map<String, String> compositeAliasesOut,
                                                 Map<String, AggregateFunction> measureAggregatesOut) {
        List<SqlBasicCall> rawAggregateCalls = new ArrayList<>();
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

            rawAggregateCalls.addAll(aggregatesInItem);

            boolean isComposite = aggregatesInItem.size() > 1 || !isSingleAggregateCall(expression);
            if (isComposite && aliasName != null) {
                compositeAliasesOut.put(aliasName, CalciteSql.unparse(expression));
            }

            // README caveat 4: record the PARSED aggregate function per output column while the AST
            // is still in hand. The merge path must never re-derive the combine op from the column
            // name — an alias like MAX(latency) AS peak_latency hides the max signal and would be
            // SUMmed across buckets. AVG is deliberately absent: it is decomposed into SUM/COUNT
            // ingredient columns whose names carry their (additive) combine op exactly.
            if (!isComposite && expression instanceof SqlBasicCall single) {
                AggregateFunction function = combineFunctionFor(single.getOperator().getName());
                if (function != null) {
                    String outputColumn = aliasName != null ? aliasName : CalciteSql.unparse(expression);
                    measureAggregatesOut.put(outputColumn, function);
                }
            }

            String spec = aliasName != null
                    ? CalciteSql.unparse(expression) + " AS " + aliasName
                    : CalciteSql.unparse(expression);
            aggregateSpecsOut.add(spec);
        }
        return rawAggregateCalls;
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

    /** Maps a parsed aggregate keyword to its cross-bucket combine op; {@code null} when not simple. */
    private AggregateFunction combineFunctionFor(String operatorName) {
        return switch (operatorName.toUpperCase(Locale.ROOT)) {
            case "SUM" -> AggregateFunction.SUM;
            case "COUNT" -> AggregateFunction.COUNT;
            case "MIN" -> AggregateFunction.MINIMUM;
            case "MAX" -> AggregateFunction.MAXIMUM;
            default -> null;
        };
    }

    // --- logic signature (HAVING / JOIN ON / DISTINCT) --------------------------------------------

    /**
     * Extracts the logic markers that are neither group-by, aggregate, filter nor projection but
     * still change the answer (README caveats 5 and 6): the {@code HAVING} expression, every
     * {@code JOIN ... ON} condition, and the {@code DISTINCT} flag. These feed the logic hash via
     * {@link CanonicalQueryObject#logicSignature()} — without them a query with {@code HAVING}
     * (or different join keys, or {@code SELECT DISTINCT}) collides on the cache key of its
     * unfiltered sibling and one of the two is served the other's rows. Full canonicalization of
     * the HAVING/ON expressions is not required for correctness here: the normalized unparse is
     * deterministic, so equal intents share a key and different intents never do (the unparse may
     * split hairs two semantically equal spellings apart, which only costs a recompute, never a
     * wrong answer).
     */
    private List<String> extractLogicSignature(SqlSelect select) {
        List<String> logicSignature = new ArrayList<>();
        if (select.isDistinct()) {
            logicSignature.add("DISTINCT");
        }
        if (select.getHaving() != null) {
            logicSignature.add("HAVING " + CalciteSql.unparse(select.getHaving()));
        }
        if (select.getFrom() != null) {
            collectJoinConditions(select.getFrom(), logicSignature);
        }
        return new ArrayList<>(new TreeSet<>(logicSignature));
    }

    private void collectJoinConditions(SqlNode from, List<String> logicSignatureOut) {
        if (from.getKind() != SqlKind.JOIN) {
            return;
        }
        org.apache.calcite.sql.SqlJoin join = (org.apache.calcite.sql.SqlJoin) from;
        collectJoinConditions(join.getLeft(), logicSignatureOut);
        collectJoinConditions(join.getRight(), logicSignatureOut);
        if (join.getCondition() != null) {
            logicSignatureOut.add("JOIN ON " + CalciteSql.unparse(join.getCondition()));
        }
    }

    // --- projection / source signatures ----------------------------------------------------------

    private List<String> extractProjectionSignature(SqlSelect select) {
        List<String> projections = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            projections.add(CalciteSql.unparse(item));
        }
        return projections;
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

    private String simpleName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1);
    }

    // --- order by / limit ------------------------------------------------------------------------

    private Optional<Integer> extractLimit(ParsedSqlQuery parsed) {
        SqlNode fetch = parsed.fetch();
        if (fetch == null) {
            return Optional.empty();
        }
        Long value = SqlNumericLiteralValue.asLong(fetch);
        if (value == null || value < 0 || value > Integer.MAX_VALUE) {
            throw new UnsupportedSqlException("LIMIT/FETCH must be a non-negative 32-bit integer literal");
        }
        return Optional.of(value.intValue());
    }

    private List<OrderByClause> extractOrderBy(ParsedSqlQuery parsed) {
        List<OrderByClause> orderBy = new ArrayList<>();
        SqlNodeList orderList = parsed.orderList();
        if (orderList == null) {
            return orderBy;
        }
        for (SqlNode element : orderList) {
            boolean ascending = true;
            Boolean nullsFirst = null;
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
                orderBy.add(OrderByClause.forColumn(simpleName(identifier), ascending, nullsFirst == null ? ascending : nullsFirst));
            } else {
                orderBy.add(OrderByClause.forExpression(CalciteSql.unparse(current), ascending, nullsFirst == null ? ascending : nullsFirst));
            }
        }
        return orderBy;
    }

}
