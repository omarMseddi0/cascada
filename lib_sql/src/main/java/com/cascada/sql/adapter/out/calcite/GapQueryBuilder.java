package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.merge.AverageIngredients;
import com.cascada.cache.domain.model.time.GapPlan;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.sql.domain.model.TimeDimensionMap;
import com.cascada.sql.exception.UnsupportedSqlException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Generates the Spark SQL needed for missing cache buckets. It coalesces adjacent ranges, replaces
 * the original time bounds while preserving other filters, and fetches mergeable ingredients.
 * Ordering and limits belong to the final merged result.
 */
public final class GapQueryBuilder {

    private static final String AVERAGE = "AVG";

    private final TimeDimensionMap timeDimensions;
    private final String timeColumn;
    private final long bucketSeconds;

    public GapQueryBuilder(String timeColumn, long bucketSeconds) {
        this(new TimeDimensionMap(Set.of(timeColumn)), timeColumn, bucketSeconds);
    }

    /**
     * Recognises bounds on every column in {@code timeDimensions} (the same set canonicalisation uses).
     * The gap range is injected on the time column the query's own window is written on, or on
     * {@code defaultTimeColumn} when the query names none.
     */
    public GapQueryBuilder(TimeDimensionMap timeDimensions, String defaultTimeColumn, long bucketSeconds) {
        this.timeDimensions = timeDimensions;
        this.timeColumn = defaultTimeColumn.toLowerCase(Locale.ROOT);
        this.bucketSeconds = bucketSeconds > 0 ? bucketSeconds : 86_400L;
    }

    /** Sorts the head, complete buckets and tail, then joins overlapping or adjacent ranges. */
    public List<TimeRange> mergeConsecutiveGaps(GapPlan gapPlan) {
        if (!gapPlan.hasGaps()) {
            return List.of();
        }

        List<TimeRange> allRanges = new ArrayList<>();
        gapPlan.head().ifPresent(allRanges::add);
        for (long bucketStart : gapPlan.body()) {
            allRanges.add(new TimeRange(bucketStart, bucketStart + bucketSeconds - 1));
        }
        gapPlan.tail().ifPresent(allRanges::add);

        allRanges.sort(Comparator.comparingLong(TimeRange::startTimestampSeconds));

        List<TimeRange> merged = new ArrayList<>();
        merged.add(allRanges.get(0));
        for (int index = 1; index < allRanges.size(); index++) {
            TimeRange current = allRanges.get(index);
            TimeRange previous = merged.get(merged.size() - 1);
            if (current.startTimestampSeconds() <= previous.endTimestampSeconds()
                    || current.startTimestampSeconds() - 1 == previous.endTimestampSeconds()) {
                merged.set(merged.size() - 1, new TimeRange(previous.startTimestampSeconds(),
                        Math.max(previous.endTimestampSeconds(), current.endTimestampSeconds())));
            } else {
                merged.add(current);
            }
        }
        return merged;
    }

    /** Rewrites the query for just the missing ranges. */
    public String buildGapQuery(String originalSql, GapPlan gapPlan) {
        List<TimeRange> gapRanges = mergeConsecutiveGaps(gapPlan);
        if (gapRanges.isEmpty()) {
            return originalSql;
        }

        SqlNode parsed = CalciteSql.parseQuery(originalSql);
        // Apply ORDER BY and LIMIT to the merged result, after fetching all gap rows.
        SqlNode root = parsed instanceof SqlOrderBy orderBy ? orderBy.query : parsed;

        if (SqlTrees.allSelects(root).stream().anyMatch(select -> SqlTrees.containsDerivedSource(select.getFrom()))) {
            throw new UnsupportedSqlException(
                    "derived-table time scopes cannot be safely rewritten for gaps");
        }

        SqlNode gapCondition = buildGapCondition(gapRanges, windowTimeColumn(root));

        if (root instanceof SqlSelect select && !containsSetOperation(root)) {
            replaceWindow(select, gapCondition);
            replaceAggregateIngredients(select);
        } else {
            injectGapIntoInnerWhere(root, gapCondition);
        }

        return CalciteSql.unparse(root);
    }

    /** Builds a bucket query at the fixed storage grain, independent of the requested display step. */
    public String buildStorageQuery(String originalSql, GapPlan gapPlan, int fixedStepSeconds) {
        SqlNode parsed = CalciteSql.parseQuery(originalSql);
        for (SqlSelect select : SqlTrees.allSelects(parsed)) {
            select.setSelectList(atStorageStep(select.getSelectList(), fixedStepSeconds, select.getParserPosition()));
            if (select.getGroup() != null) {
                select.setGroupBy(atStorageStep(select.getGroup(), fixedStepSeconds, select.getParserPosition()));
            }
        }
        return buildGapQuery(CalciteSql.unparse(parsed), gapPlan);
    }

    private SqlNodeList atStorageStep(SqlNodeList expressions, int fixedStepSeconds, SqlParserPos position) {
        SqlNodeList rewritten = new SqlNodeList(position);
        for (SqlNode expression : expressions) {
            rewritten.add(TimeStepNormalizer.bucketOutput(expression, timeDimensions, fixedStepSeconds));
        }
        return rewritten;
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
    private void replaceAggregateIngredients(SqlSelect select) {
        Set<String> emittedIngredients = new HashSet<>();
        List<SqlNode> expanded = new ArrayList<>();
        for (SqlNode item : select.getSelectList()) {
            if (item.getKind() == SqlKind.AS && item instanceof SqlBasicCall alias
                    && alias.operand(0) instanceof SqlBasicCall expression
                    && SqlTrees.ARITHMETIC.contains(expression.getKind())
                    && SqlTrees.containsAggregate(expression)) {
                expandIngredients(expression, expanded, emittedIngredients);
            } else {
                expanded.add(item);
            }
        }
        SqlNodeList rewritten = new SqlNodeList(SqlParserPos.ZERO);
        for (SqlNode item : expanded) {
            SqlNode expression = SqlTrees.unaliased(item);
            if (expression != item && expression instanceof SqlBasicCall average && isAverageCall(average)) {
                addAverageIngredients(average, SqlTrees.outputName(item), rewritten);
            } else {
                rewritten.add(item);
            }
        }
        select.setSelectList(rewritten);
    }

    private void addAverageIngredients(SqlBasicCall average, String name, SqlNodeList projections) {
        if (average.operandCount() != 1 || average.getFunctionQuantifier() != null) {
            throw new UnsupportedSqlException("AVG takes exactly one plain argument in cached queries");
        }
        String argument = CalciteSql.unparse(average.operand(0));
        projections.add(aliased(AggregateNormalizer.sumIngredient(argument), AverageIngredients.sumColumn(name)));
        projections.add(aliased(AggregateNormalizer.countIngredient(argument), AverageIngredients.countColumn(name)));
        projections.add(aliased("MIN(TYPEOF(" + argument + "))", AverageIngredients.typeColumn(name)));
    }

    private void expandIngredients(SqlNode node, List<SqlNode> expanded, Set<String> emitted) {
        if (!(node instanceof SqlBasicCall call)) return;
        if (SqlTrees.isAggregateCall(call)) {
            String expression = CalciteSql.unparse(call);
            if (emitted.add(expression)) expanded.add(aliased(expression, CompositeIngredientNames.alias(expression)));
            return;
        }
        for (SqlNode operand : call.getOperandList()) expandIngredients(operand, expanded, emitted);
    }

    private static SqlNode aliased(String expression, String name) {
        return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, CalciteSql.parseExpression(expression),
                new SqlIdentifier(name, SqlParserPos.ZERO));
    }

    /** Removes the original window bounds so warming can inject a different range. */
    public String stripTimeConditions(String sql) {
        SqlNode parsed = CalciteSql.parseQuery(sql);
        SqlNode root = parsed instanceof SqlOrderBy orderBy ? orderBy.query : parsed;
        for (SqlSelect select : SqlTrees.allSelects(root)) {
            SqlNode where = select.getWhere();
            if (where != null) {
                select.setWhere(filterOutTimeConditions(where));
            }
        }
        return CalciteSql.unparse(root);
    }


    private void replaceWindow(SqlSelect select, SqlNode gapCondition) {
        SqlNode remainder = filterOutTimeConditions(select.getWhere());
        select.setWhere(remainder == null ? cloneCondition(gapCondition) : and(remainder, cloneCondition(gapCondition)));
    }


    private void injectGapIntoInnerWhere(SqlNode root, SqlNode gapCondition) {
        List<SqlSelect> innerSelects = findInnermostSelectsWithFrom(root);
        for (SqlSelect select : innerSelects) {
            replaceWindow(select, gapCondition);
        }
    }

    private boolean containsSetOperation(SqlNode node) {
        if (node == null) {
            return false;
        }
        return SqlTrees.isSetOperation(node)
                || node instanceof SqlCall call && call.getOperandList().stream().anyMatch(this::containsSetOperation);
    }

    private List<SqlSelect> findInnermostSelectsWithFrom(SqlNode root) {
        return SqlTrees.allSelects(root).stream()
                .filter(select -> readsDirectlyFromTable(select.getFrom()))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private boolean readsDirectlyFromTable(SqlNode from) {
        SqlNode source = SqlTrees.stripAlias(from);
        // Subquery wrappers cannot see the raw time column.
        return source instanceof SqlIdentifier
                || source instanceof SqlJoin join && joinTouchesTable(join);
    }

    private boolean joinTouchesTable(SqlJoin join) {
        return SqlTrees.stripAlias(join.getLeft()) instanceof SqlIdentifier
                || SqlTrees.stripAlias(join.getRight()) instanceof SqlIdentifier;
    }


    private SqlNode filterOutTimeConditions(SqlNode condition) {
        // OR, NOT, functions and time-dependent business filters retain their original meaning; only
        // top-level conjuncts the canonicaliser read as the time window are dropped.
        SqlNode kept = null;
        for (SqlNode conjunct : SqlTrees.conjuncts(condition)) {
            if (!isRangeBound(conjunct)) {
                kept = combine(SqlKind.AND, kept, conjunct);
            }
        }
        return kept;
    }

    private boolean isRangeBound(SqlNode node) {
        return TimeBoundClassifier.classify(node, timeDimensions) != null;
    }

    /** The time column the query's window bounds are written on; the configured column otherwise. */
    private String windowTimeColumn(SqlNode root) {
        for (SqlSelect select : SqlTrees.allSelects(root)) {
            for (SqlNode conjunct : SqlTrees.conjuncts(select.getWhere())) {
                if (isRangeBound(conjunct)) {
                    String[] column = {null};
                    SqlTrees.forEachColumnReference(conjunct, identifier -> {
                        if (column[0] == null && SqlTrees.isTimeColumn(identifier, timeDimensions)) {
                            column[0] = SqlTrees.lastName(identifier);
                        }
                    });
                    if (column[0] != null) return column[0];
                }
            }
        }
        return timeColumn;
    }


    private SqlNode buildGapCondition(List<TimeRange> ranges, String column) {
        SqlNode condition = null;
        for (TimeRange range : ranges) {
            SqlNode lower = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL.createCall(SqlParserPos.ZERO,
                    new SqlIdentifier(column, SqlParserPos.ZERO),
                    SqlLiteral.createExactNumeric(
                            Long.toString(range.startTimestampSeconds()), SqlParserPos.ZERO));
            SqlNode upper = SqlStdOperatorTable.LESS_THAN_OR_EQUAL.createCall(SqlParserPos.ZERO,
                    new SqlIdentifier(column, SqlParserPos.ZERO),
                    SqlLiteral.createExactNumeric(
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

    /** Gives each rewritten SELECT its own predicate node. */
    private SqlNode cloneCondition(SqlNode gapCondition) {
        return gapCondition.clone(SqlParserPos.ZERO);
    }
}
