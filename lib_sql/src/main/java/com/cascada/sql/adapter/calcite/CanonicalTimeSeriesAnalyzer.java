package com.cascada.sql.adapter.calcite;

import com.cascada.cache.domain.time.TimeRange;
import com.cascada.sql.domain.TimeDimensionMap;
import com.cascada.sql.domain.UnsupportedSqlException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/** Extracts the one time window and time-series shape supported by cache canonicalization. */
final class CanonicalTimeSeriesAnalyzer {

    CanonicalTimeRangeExtraction extractTimeRangeAndFilters(SqlSelect select, TimeDimensionMap dimensions) {
        List<SqlNode> conjuncts = new ArrayList<>();
        flattenAndConjuncts(select.getWhere(), conjuncts);

        Long start = null;
        Long end = null;
        List<String> filters = new ArrayList<>();

        for (SqlNode conjunct : conjuncts) {
            CanonicalTimeBound bound = asTimeBound(conjunct, dimensions);
            if (bound == null) {
                filters.add(CalciteSql.unparse(conjunct));
                continue;
            }
            if (bound.start() != null) {
                start = start == null ? bound.start() : Math.max(start, bound.start());
            }
            if (bound.end() != null) {
                end = end == null ? bound.end() : Math.min(end, bound.end());
            }
        }

        Optional<TimeRange> timeRange = start != null && end != null && end >= start
                ? Optional.of(new TimeRange(start, end))
                : Optional.empty();
        return new CanonicalTimeRangeExtraction(timeRange, new ArrayList<>(new TreeSet<>(filters)));
    }

    CanonicalTimeSeriesShape detectTimeSeries(SqlSelect select, TimeDimensionMap dimensions) {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return new CanonicalTimeSeriesShape(false, Optional.empty(), false);
        }
        for (SqlNode expression : group) {
            Optional<Integer> step = floorBucketStep(expression, dimensions);
            if (step.isPresent()) {
                return new CanonicalTimeSeriesShape(true, step, false);
            }
        }
        for (SqlNode expression : group) {
            if (expression instanceof SqlIdentifier identifier && isTimeColumn(identifier, dimensions)) {
                return new CanonicalTimeSeriesShape(true, Optional.empty(), true);
            }
        }
        return new CanonicalTimeSeriesShape(false, Optional.empty(), false);
    }

    static Optional<Integer> floorBucketStep(SqlNode expression, TimeDimensionMap dimensions) {
        SqlNode node = unwrapCast(expression);
        if (node.getKind() != SqlKind.TIMES || !(node instanceof SqlBasicCall times)) {
            return Optional.empty();
        }
        SqlNode left = times.operand(0);
        SqlNode right = times.operand(1);
        SqlNode floor = left.getKind() == SqlKind.FLOOR ? left : right.getKind() == SqlKind.FLOOR ? right : null;
        if (floor == null || !(floor instanceof SqlBasicCall floorCall)) {
            return Optional.empty();
        }
        SqlNode multiplierNode = floor == left ? right : left;
        SqlNode divide = floorCall.operand(0);
        if (divide.getKind() != SqlKind.DIVIDE || !(divide instanceof SqlBasicCall division)) {
            return Optional.empty();
        }
        if (!(division.operand(0) instanceof SqlIdentifier column) || !isTimeColumn(column, dimensions)) {
            return Optional.empty();
        }
        Long step = SqlNumericLiteralValue.asLong(division.operand(1));
        Long multiplier = SqlNumericLiteralValue.asLong(multiplierNode);
        if (step == null || multiplier == null || step <= 0 || step > Integer.MAX_VALUE
                || !step.equals(multiplier)) {
            return Optional.empty();
        }
        return Optional.of(step.intValue());
    }

    private CanonicalTimeBound asTimeBound(SqlNode conjunct, TimeDimensionMap dimensions) {
        if (!(conjunct instanceof SqlBasicCall call)) {
            return null;
        }
        return switch (conjunct.getKind()) {
            case GREATER_THAN_OR_EQUAL -> comparisonBound(call, dimensions, true, false);
            case GREATER_THAN -> comparisonBound(call, dimensions, true, true);
            case LESS_THAN_OR_EQUAL -> comparisonBound(call, dimensions, false, false);
            case LESS_THAN -> comparisonBound(call, dimensions, false, true);
            case BETWEEN -> betweenBound(call, dimensions);
            default -> null;
        };
    }

    private CanonicalTimeBound betweenBound(SqlBasicCall call, TimeDimensionMap dimensions) {
        if (call.getOperandList().size() != 3
                || !(call.operand(0) instanceof SqlIdentifier column)
                || !isTimeColumn(column, dimensions)) {
            return null;
        }
        Long lower = SqlNumericLiteralValue.asLong(call.operand(1));
        Long upper = SqlNumericLiteralValue.asLong(call.operand(2));
        return lower == null || upper == null ? null : new CanonicalTimeBound(lower, upper);
    }

    private CanonicalTimeBound comparisonBound(SqlBasicCall call, TimeDimensionMap dimensions,
                                               boolean greater, boolean strict) {
        SqlNode left = call.operand(0);
        SqlNode right = call.operand(1);
        boolean isStart;
        Long value;
        if (left instanceof SqlIdentifier identifier && isTimeColumn(identifier, dimensions)) {
            value = SqlNumericLiteralValue.asLong(right);
            isStart = greater;
        } else if (right instanceof SqlIdentifier identifier && isTimeColumn(identifier, dimensions)) {
            value = SqlNumericLiteralValue.asLong(left);
            isStart = !greater;
        } else {
            return null;
        }
        if (value == null) {
            return null;
        }

        long inclusive = value;
        if (strict) {
            try {
                inclusive = isStart ? Math.addExact(value, 1L) : Math.subtractExact(value, 1L);
            } catch (ArithmeticException overflow) {
                throw new UnsupportedSqlException(
                        "strict time comparison exceeds the supported epoch range", overflow);
            }
        }
        return isStart ? new CanonicalTimeBound(inclusive, null) : new CanonicalTimeBound(null, inclusive);
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

    private static boolean isTimeColumn(SqlIdentifier identifier, TimeDimensionMap dimensions) {
        return dimensions.isTimeColumn(simpleName(identifier));
    }

    private static String simpleName(SqlIdentifier identifier) {
        return identifier.names.get(identifier.names.size() - 1);
    }

    private static SqlNode unwrapCast(SqlNode node) {
        if (node.getKind() == SqlKind.CAST && node instanceof SqlBasicCall cast) {
            return cast.operand(0);
        }
        return node;
    }
}
