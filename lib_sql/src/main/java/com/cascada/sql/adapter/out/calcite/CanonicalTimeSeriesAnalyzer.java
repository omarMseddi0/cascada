package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.time.TimeRange;
import com.cascada.sql.domain.TimeDimensionMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

/** Extracts the one time window and time-series shape supported by cache canonicalization. */
final class CanonicalTimeSeriesAnalyzer {

    CanonicalTimeRangeExtraction extractTimeRangeAndFilters(SqlSelect select, TimeDimensionMap dimensions) {
        List<SqlNode> conjuncts = new ArrayList<>();
        flattenAndConjuncts(select.getWhere(), conjuncts);

        Long start = null;
        Long end = null;
        List<String> filters = new ArrayList<>();

        for (SqlNode conjunct : conjuncts) {
            CanonicalTimeBound bound = TimeBoundClassifier.classify(conjunct, dimensions);
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
        SqlNode floor;
        SqlNode multiplierNode;
        if (left.getKind() == SqlKind.FLOOR) {
            floor = left;
            multiplierNode = right;
        } else if (right.getKind() == SqlKind.FLOOR) {
            floor = right;
            multiplierNode = left;
        } else {
            return Optional.empty();
        }
        if (!(floor instanceof SqlBasicCall floorCall)) {
            return Optional.empty();
        }
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
