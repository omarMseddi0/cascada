package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.merge.AverageIngredients;
import com.cascada.cache.domain.time.GapPlan;
import com.cascada.cache.domain.time.TimeRange;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Surgically rewrites a query so Spark fetches <em>only</em> the gap (the buckets not in cache),
 * ported from {@code GapQueryBuilder} in {@code sql_rewriter.py} — now on <b>Apache Calcite</b>
 * instead of JSqlParser. Faithful port of {@code _redefine_spark_query_for_gaps} and its helpers:
 *
 * <ul>
 *   <li>{@link #mergeConsecutiveGaps(GapPlan)} — verbatim {@code _merge_consecutive_gaps}: coalesce
 *       head + whole body days + tail into the fewest contiguous {@code [start, end]} ranges
 *       (adjacent where {@code prevEnd + 1 >= currentStart});</li>
 *   <li>{@link #buildGapQuery(String, GapPlan)} — build an {@code OR} of the merged ranges on the time
 *       column and inject it, choosing the structure-aware path exactly like the Python: <em>flat</em>
 *       SQL replaces the existing time-only predicate (or ANDs the gap onto non-time predicates), while
 *       <em>UNION/subquery</em> SQL pushes the gap into the innermost data-reading {@code WHERE}s.
 *       {@code ORDER BY}/{@code LIMIT} are dropped from the gap fetch (the Python transformer removes
 *       {@code exp.Order}/{@code exp.Limit});</li>
 *   <li>{@link #stripTimeConditions(String)} — verbatim {@code _strip_time_conditions_from_sql} /
 *       {@code _filter_out_time_conditions}: drop time predicates so a fresh range can be injected.</li>
 * </ul>
 *
 * <p>The emitted predicate is the plain {@code timeCol >= N AND timeCol <= M} shape (Calcite quotes
 * an identifier only when required), which downstream Spark and the cache-correctness oracle both read.
 */
public final class GapQueryBuilder {

    private static final String AVERAGE = "AVG";

    private final String timeColumn;
    private final long bucketSeconds;

    public GapQueryBuilder(String timeColumn, long bucketSeconds) {
        this.timeColumn = timeColumn.toLowerCase(Locale.ROOT);
        this.bucketSeconds = bucketSeconds > 0 ? bucketSeconds : 86_400L;
    }

    /** Ported from {@code _merge_consecutive_gaps}: head + body-day ranges + tail, sorted and coalesced. */
    public List<TimeRange> mergeConsecutiveGaps(GapPlan gapPlan) {
        if (!gapPlan.hasGaps()) {
            return List.of();
        }

        List<TimeRange> allRanges = new ArrayList<>();
        gapPlan.head().ifPresent(allRanges::add);
        for (long dayStart : gapPlan.body()) {
            allRanges.add(new TimeRange(dayStart, dayStart + bucketSeconds - 1));
        }
        gapPlan.tail().ifPresent(allRanges::add);

        allRanges.sort(Comparator.comparingLong(TimeRange::startTimestampSeconds));

        List<TimeRange> merged = new ArrayList<>();
        merged.add(allRanges.get(0));
        for (int index = 1; index < allRanges.size(); index++) {
            TimeRange current = allRanges.get(index);
            TimeRange previous = merged.get(merged.size() - 1);
            if (previous.endTimestampSeconds() + 1 >= current.startTimestampSeconds()) {
                merged.set(merged.size() - 1, new TimeRange(previous.startTimestampSeconds(),
                        Math.max(previous.endTimestampSeconds(), current.endTimestampSeconds())));
            } else {
                merged.add(current);
            }
        }
        return merged;
    }

    /** Ported from {@code _redefine_spark_query_for_gaps}: structure-aware injection of the gap filter. */
    public String buildGapQuery(String originalSql, GapPlan gapPlan) {
        List<TimeRange> gapRanges = mergeConsecutiveGaps(gapPlan);
        if (gapRanges.isEmpty()) {
            return originalSql;
        }

        SqlNode parsed = CalciteSql.parseQuery(originalSql);
        // Drop ORDER BY / LIMIT from the gap fetch (Python removes exp.Order / exp.Limit).
        SqlNode root = parsed instanceof SqlOrderBy orderBy ? orderBy.query : parsed;

        if (allSelects(root).stream().anyMatch(select -> isSubquerySource(select.getFrom()))) {
            throw new com.cascada.sql.exception.UnsupportedSqlException(
                    "derived-table time scopes cannot be safely rewritten for gaps");
        }

        SqlNode gapCondition = buildGapCondition(gapRanges);

        if (isUnionOrSubqueryStructure(root)) {
            injectGapIntoInnerWhere(root, gapCondition);
        } else if (root instanceof SqlSelect select) {
            applyFlatGap(select, gapCondition);
            replaceAverages(select);
        } else {
            injectGapIntoInnerWhere(root, gapCondition);
        }

        return CalciteSql.unparse(root);
    }

    /** True for {@code AVG(...)}. Gap queries fetch its SUM and COUNT ingredients instead. */
    static boolean isAverageCall(SqlBasicCall call) {
        return call.getOperator().getName().equalsIgnoreCase(AVERAGE);
    }

    /**
     * {@code AVG(x) AS a} becomes {@code SUM(x)}, {@code COUNT(x)} and {@code MIN(TYPEOF(x))} under
     * {@link AverageIngredients} names. Buckets store these ingredients; the cache merges them and
     * divides at read time, because an average of per-bucket averages is not the average.
     */
    private void replaceAverages(SqlSelect select) {
        SqlNodeList rewritten = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode item : select.getSelectList()) {
            boolean aliasedAverage = item.getKind() == SqlKind.AS && item instanceof SqlBasicCall alias
                    && alias.operand(0) instanceof SqlBasicCall call && isAverageCall(call);
            if (!aliasedAverage) {
                rewritten.add(item);
                continue;
            }
            SqlBasicCall alias = (SqlBasicCall) item;
            SqlBasicCall average = (SqlBasicCall) alias.operand(0);
            if (average.operandCount() != 1 || average.getFunctionQuantifier() != null) {
                throw new com.cascada.sql.exception.UnsupportedSqlException(
                        "AVG takes exactly one plain argument in cached queries");
            }
            String argument = CalciteSql.unparse(average.operand(0));
            String name = CalciteSql.unparse(alias.operand(1)).replace("`", "");
            rewritten.add(aliased("SUM(" + argument + ")", AverageIngredients.sumColumn(name)));
            rewritten.add(aliased("COUNT(" + argument + ")", AverageIngredients.countColumn(name)));
            rewritten.add(aliased("MIN(TYPEOF(" + argument + "))", AverageIngredients.typeColumn(name)));
        }
        select.setSelectList(rewritten);
    }

    private static SqlNode aliased(String expression, String name) {
        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, CalciteSql.parseExpression(expression),
                new SqlIdentifier(name, SqlParserPos.ZERO));
    }

    /** Ported from {@code strip_time_conditions}: drop time predicates so a new range can be injected. */
    public String stripTimeConditions(String sql) {
        SqlNode parsed = CalciteSql.parseQuery(sql);
        SqlNode root = parsed instanceof SqlOrderBy orderBy ? orderBy.query : parsed;
        for (SqlSelect select : allSelects(root)) {
            SqlNode where = select.getWhere();
            if (where != null) {
                select.setWhere(filterOutTimeConditions(where));
            }
        }
        return CalciteSql.unparse(root);
    }

    // --- flat path (ported from the else branch of _redefine_spark_query_for_gaps) ----------------

    private void applyFlatGap(SqlSelect select, SqlNode gapCondition) {
        SqlNode remainder = filterOutTimeConditions(select.getWhere());
        select.setWhere(remainder == null ? cloneCondition(gapCondition) : and(remainder, cloneCondition(gapCondition)));
    }

    // --- union / subquery path (ported from _inject_gap_into_inner_where) -------------------------

    private void injectGapIntoInnerWhere(SqlNode root, SqlNode gapCondition) {
        List<SqlSelect> innerSelects = findInnermostSelectsWithFrom(root);
        for (SqlSelect select : innerSelects) {
            applyFlatGap(select, gapCondition);
        }
    }

    /** Ported from {@code _is_union_or_subquery_structure}. */
    private boolean isUnionOrSubqueryStructure(SqlNode root) {
        if (containsUnion(root)) {
            return true;
        }
        return allSelects(root).stream()
                .anyMatch(select -> isSubquerySource(select.getFrom()));
    }

    private boolean containsUnion(SqlNode node) {
        if (node == null) {
            return false;
        }
        if (node.getKind() == SqlKind.UNION || node.getKind() == SqlKind.INTERSECT
                || node.getKind() == SqlKind.EXCEPT) {
            return true;
        }
        return node instanceof SqlCall call
                && call.getOperandList().stream().anyMatch(this::containsUnion);
    }

    private boolean isSubquerySource(SqlNode from) {
        SqlNode source = stripAlias(from);
        return source instanceof SqlSelect || source instanceof SqlOrderBy
                || (source != null && (source.getKind() == SqlKind.UNION
                || source.getKind() == SqlKind.INTERSECT || source.getKind() == SqlKind.EXCEPT));
    }

    /** Ported from {@code _find_innermost_selects_with_from}: only SELECTs reading directly from a table. */
    private List<SqlSelect> findInnermostSelectsWithFrom(SqlNode root) {
        return allSelects(root).stream()
                .filter(select -> readsDirectlyFromTable(select.getFrom()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean readsDirectlyFromTable(SqlNode from) {
        SqlNode source = stripAlias(from);
        // Subquery wrappers cannot see the raw time column.
        return source instanceof SqlIdentifier
                || source instanceof SqlJoin join && joinTouchesTable(join);
    }

    private boolean joinTouchesTable(SqlJoin join) {
        return stripAlias(join.getLeft()) instanceof SqlIdentifier
                || stripAlias(join.getRight()) instanceof SqlIdentifier;
    }

    private SqlNode stripAlias(SqlNode from) {
        if (from != null && from.getKind() == SqlKind.AS && from instanceof SqlBasicCall call) {
            return call.operand(0);
        }
        return from;
    }

    // --- time predicate analysis (ported from _has_time_only_subtree / _replace_time_condition) ---

    /** Ported from {@code _filter_out_time_conditions}: drop {@code timeCol >/>=/</<=} predicates. */
    private SqlNode filterOutTimeConditions(SqlNode condition) {
        if (condition == null || isRangeBound(condition)) {
            return null;
        }
        if (condition.getKind() == SqlKind.AND && condition instanceof SqlBasicCall call) {
            return combine(SqlKind.AND, filterOutTimeConditions(call.operand(0)),
                    filterOutTimeConditions(call.operand(1)));
        }
        // OR, NOT, functions and time-dependent business filters retain their original meaning.
        return condition;
    }

    private boolean isRangeBound(SqlNode node) {
        return TimeBoundClassifier.classify(node,
                new com.cascada.sql.domain.TimeDimensionMap(java.util.Set.of(timeColumn))) != null;
    }

    // --- node construction -----------------------------------------------------------------------

    private SqlNode buildGapCondition(List<TimeRange> ranges) {
        SqlNode condition = null;
        for (TimeRange range : ranges) {
            SqlNode lower = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL.createCall(SqlParserPos.ZERO,
                    new SqlIdentifier(timeColumn, SqlParserPos.ZERO),
                    org.apache.calcite.sql.SqlLiteral.createExactNumeric(
                            Long.toString(range.startTimestampSeconds()), SqlParserPos.ZERO));
            SqlNode upper = SqlStdOperatorTable.LESS_THAN_OR_EQUAL.createCall(SqlParserPos.ZERO,
                    new SqlIdentifier(timeColumn, SqlParserPos.ZERO),
                    org.apache.calcite.sql.SqlLiteral.createExactNumeric(
                            Long.toString(range.endTimestampSeconds()), SqlParserPos.ZERO));
            condition = combine(SqlKind.OR, condition, and(lower, upper));
        }
        return condition;
    }

    private SqlNode and(SqlNode left, SqlNode right) {
        return SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, left, right);
    }

    private SqlNode combine(SqlKind kind, SqlNode left, SqlNode right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return (kind == SqlKind.OR ? SqlStdOperatorTable.OR : SqlStdOperatorTable.AND)
                .createCall(SqlParserPos.ZERO, left, right);
    }

    /** Re-parse the gap predicate so each injection site gets an independent subtree (Python copies). */
    private SqlNode cloneCondition(SqlNode gapCondition) {
        return gapCondition.clone(SqlParserPos.ZERO);
    }

    // --- tree walk -------------------------------------------------------------------------------

    private List<SqlSelect> allSelects(SqlNode root) {
        List<SqlSelect> selects = new ArrayList<>();
        collectSelects(root, selects);
        return selects;
    }

    private void collectSelects(SqlNode node, List<SqlSelect> collector) {
        if (node == null) {
            return;
        }
        if (node instanceof SqlSelect select) {
            collector.add(select);
            collectSelects(select.getFrom(), collector);
            return;
        }
        if (node instanceof SqlOrderBy orderBy) {
            collectSelects(orderBy.query, collector);
            return;
        }
        if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                collectSelects(operand, collector);
            }
        }
    }
}
