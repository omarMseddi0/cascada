package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.domain.model.TimeDimensionMap;
import com.cascada.sql.exception.UnsupportedSqlException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** One classifier for integer epoch bounds used by both canonicalization and gap rewriting. */
final class TimeBoundClassifier {
    private TimeBoundClassifier() { }

    static CanonicalTimeBound classify(SqlNode conjunct, TimeDimensionMap dimensions) {
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

    private static CanonicalTimeBound betweenBound(SqlBasicCall call, TimeDimensionMap dimensions) {
        if (call.getOperator() != SqlStdOperatorTable.BETWEEN
                || call.getOperandList().size() != 3
                || !(call.operand(0) instanceof SqlIdentifier column)
                || !isTimeColumn(column, dimensions)) {
            return null;
        }
        Long lower = SqlNumericLiteralValue.asLong(call.operand(1));
        Long upper = SqlNumericLiteralValue.asLong(call.operand(2));
        return lower == null || upper == null || lower > upper ? null : new CanonicalTimeBound(lower, upper);
    }

    private static CanonicalTimeBound comparisonBound(SqlBasicCall call, TimeDimensionMap dimensions,
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

    private static boolean isTimeColumn(SqlIdentifier identifier, TimeDimensionMap dimensions) {
        return SqlTrees.isTimeColumn(identifier, dimensions);
    }
}
