package com.cascada.sql.rewrite;

import com.cascada.cache.domain.GapPlan;
import com.cascada.cache.domain.TimeRange;
import com.cascada.sql.calcite.CalciteSql;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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

        allRanges.sort((left, right) -> Long.compare(left.startTimestampSeconds(), right.startTimestampSeconds()));

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

        SqlNode gapCondition = buildGapCondition(gapRanges);

        if (isUnionOrSubqueryStructure(root)) {
            injectGapIntoInnerWhere(root, gapCondition);
        } else if (root instanceof SqlSelect select) {
            applyFlatGap(select, gapCondition);
        } else {
            injectGapIntoInnerWhere(root, gapCondition);
        }

        return CalciteSql.unparse(root);
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
        SqlNode existing = select.getWhere();
        if (existing == null) {
            select.setWhere(gapCondition);
            return;
        }
        if (hasTimeOnlySubtree(existing)) {
            select.setWhere(replaceTimeCondition(existing, gapCondition));
        } else {
            select.setWhere(and(existing, gapCondition));
        }
    }

    // --- union / subquery path (ported from _inject_gap_into_inner_where) -------------------------

    private void injectGapIntoInnerWhere(SqlNode root, SqlNode gapCondition) {
        List<SqlSelect> innerSelects = findInnermostSelectsWithFrom(root);
        for (SqlSelect select : innerSelects) {
            SqlNode existing = select.getWhere();
            if (existing == null) {
                select.setWhere(cloneCondition(gapCondition));
            } else if (hasTimeOnlySubtree(existing)) {
                select.setWhere(replaceTimeCondition(existing, cloneCondition(gapCondition)));
            } else {
                select.setWhere(and(existing, cloneCondition(gapCondition)));
            }
        }
    }

    /** Ported from {@code _is_union_or_subquery_structure}. */
    private boolean isUnionOrSubqueryStructure(SqlNode root) {
        if (containsUnion(root)) {
            return true;
        }
        for (SqlSelect select : allSelects(root)) {
            if (isSubquerySource(select.getFrom())) {
                return true;
            }
        }
        return false;
    }

    private boolean containsUnion(SqlNode node) {
        if (node == null) {
            return false;
        }
        if (node.getKind() == SqlKind.UNION || node.getKind() == SqlKind.INTERSECT
                || node.getKind() == SqlKind.EXCEPT) {
            return true;
        }
        if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (containsUnion(operand)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isSubquerySource(SqlNode from) {
        SqlNode source = stripAlias(from);
        return source instanceof SqlSelect || source instanceof SqlOrderBy
                || (source != null && (source.getKind() == SqlKind.UNION
                || source.getKind() == SqlKind.INTERSECT || source.getKind() == SqlKind.EXCEPT));
    }

    /** Ported from {@code _find_innermost_selects_with_from}: only SELECTs reading directly from a table. */
    private List<SqlSelect> findInnermostSelectsWithFrom(SqlNode root) {
        List<SqlSelect> innermost = new ArrayList<>();
        for (SqlSelect select : allSelects(root)) {
            SqlNode from = select.getFrom();
            if (from == null) {
                continue;
            }
            SqlNode source = stripAlias(from);
            if (source instanceof SqlIdentifier) {
                innermost.add(select);
            } else if (source != null && source.getKind() == SqlKind.JOIN) {
                if (joinTouchesTable((SqlJoin) source)) {
                    innermost.add(select);
                }
            }
            // FROM (subquery) AS alias is a wrapper that cannot see raw columns like ts: skip.
        }
        return innermost;
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

    private boolean hasTimeOnlySubtree(SqlNode condition) {
        if (condition == null) {
            return false;
        }
        List<SqlIdentifier> identifiers = new ArrayList<>();
        collectIdentifiers(condition, identifiers);
        if (!identifiers.isEmpty() && identifiers.stream().allMatch(this::isTimeColumn)) {
            return true;
        }
        if (condition instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null && hasTimeOnlySubtree(operand)) {
                    return true;
                }
            }
        }
        return false;
    }

    private SqlNode replaceTimeCondition(SqlNode condition, SqlNode gapCondition) {
        if (condition == null) {
            return gapCondition;
        }
        if (isTimeOnlyPredicate(condition)) {
            return cloneCondition(gapCondition);
        }
        SqlKind kind = condition.getKind();
        if ((kind == SqlKind.AND || kind == SqlKind.OR) && condition instanceof SqlBasicCall call) {
            SqlNode left = replaceTimeCondition(call.operand(0), gapCondition);
            SqlNode right = replaceTimeCondition(call.operand(1), gapCondition);
            return combine(kind, left, right);
        }
        return condition;
    }

    /** Ported from {@code _filter_out_time_conditions}: drop {@code timeCol >/>=/</<=} predicates. */
    private SqlNode filterOutTimeConditions(SqlNode condition) {
        if (condition == null) {
            return null;
        }
        switch (condition.getKind()) {
            case GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL -> {
                return referencesTimeColumn(condition) ? null : condition;
            }
            case AND, OR -> {
                SqlBasicCall call = (SqlBasicCall) condition;
                SqlNode left = filterOutTimeConditions(call.operand(0));
                SqlNode right = filterOutTimeConditions(call.operand(1));
                return combine(condition.getKind(), left, right);
            }
            default -> {
                return condition;
            }
        }
    }

    private boolean isTimeOnlyPredicate(SqlNode node) {
        List<SqlIdentifier> identifiers = new ArrayList<>();
        collectIdentifiers(node, identifiers);
        return !identifiers.isEmpty() && identifiers.stream().allMatch(this::isTimeColumn);
    }

    private boolean referencesTimeColumn(SqlNode node) {
        List<SqlIdentifier> identifiers = new ArrayList<>();
        collectIdentifiers(node, identifiers);
        return identifiers.stream().anyMatch(this::isTimeColumn);
    }

    private void collectIdentifiers(SqlNode node, List<SqlIdentifier> collector) {
        if (node instanceof SqlIdentifier identifier) {
            collector.add(identifier);
        } else if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    collectIdentifiers(operand, collector);
                }
            }
        }
    }

    private boolean isTimeColumn(SqlIdentifier identifier) {
        String name = identifier.names.get(identifier.names.size() - 1);
        return name.toLowerCase(Locale.ROOT).equals(timeColumn);
    }

    // --- node construction -----------------------------------------------------------------------

    private SqlNode buildGapCondition(List<TimeRange> ranges) {
        StringBuilder predicate = new StringBuilder();
        for (int index = 0; index < ranges.size(); index++) {
            if (index > 0) {
                predicate.append(" OR ");
            }
            TimeRange range = ranges.get(index);
            predicate.append('(').append(timeColumn).append(" >= ").append(range.startTimestampSeconds())
                    .append(" AND ").append(timeColumn).append(" <= ").append(range.endTimestampSeconds())
                    .append(')');
        }
        return CalciteSql.parseExpression(predicate.toString());
    }

    private SqlNode and(SqlNode left, SqlNode right) {
        return SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, left, right);
    }

    private SqlNode combine(SqlKind kind, SqlNode left, SqlNode right) {
        if (left == null && right == null) {
            return null;
        }
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
        return CalciteSql.parseExpression(CalciteSql.unparse(gapCondition));
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
