package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import java.util.Optional;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * The epoch time-bucket expression {@code [CAST(] FLOOR(t / N) * N [AS type)]}: recognised once and
 * rebuilt once, so canonical identity, the fixed storage step and logical-table bucketing all agree on
 * its shape and keep its result type.
 */
final class TimeBuckets {

    private TimeBuckets() {
    }

    static Optional<TimeBucket> match(SqlNode expression, TimeDimensionMap dimensions) {
        SqlDataTypeSpec castType = null;
        SqlNode node = expression;
        if (node.getKind() == SqlKind.CAST && node instanceof SqlBasicCall cast
                && cast.operand(1) instanceof SqlDataTypeSpec type) {
            castType = type;
            node = cast.operand(0);
        }
        if (node.getKind() != SqlKind.TIMES || !(node instanceof SqlBasicCall times)) {
            return Optional.empty();
        }
        SqlNode floor;
        SqlNode multiplierNode;
        if (times.operand(0).getKind() == SqlKind.FLOOR) {
            floor = times.operand(0);
            multiplierNode = times.operand(1);
        } else if (times.operand(1).getKind() == SqlKind.FLOOR) {
            floor = times.operand(1);
            multiplierNode = times.operand(0);
        } else {
            return Optional.empty();
        }
        if (!(floor instanceof SqlBasicCall floorCall)
                || floorCall.operand(0).getKind() != SqlKind.DIVIDE
                || !(floorCall.operand(0) instanceof SqlBasicCall division)
                || !(division.operand(0) instanceof SqlIdentifier column)
                || !SqlTrees.isTimeColumn(column, dimensions)) {
            return Optional.empty();
        }
        Long step = SqlNumericLiteralValue.asLong(division.operand(1));
        Long multiplier = SqlNumericLiteralValue.asLong(multiplierNode);
        if (step == null || multiplier == null || step <= 0 || step > Integer.MAX_VALUE || !step.equals(multiplier)) {
            return Optional.empty();
        }
        return Optional.of(new TimeBucket(column, step.intValue(), castType));
    }

    /** {@code CAST(FLOOR(column / step) * step AS castType)}, or without the CAST when none is given. */
    static SqlNode build(SqlIdentifier column, int step, SqlDataTypeSpec castType) {
        SqlNode bucket = CalciteSql.parseExpression(
                "FLOOR(" + CalciteSql.unparse(column) + " / " + step + ") * " + step);
        return castType == null ? bucket
                : SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, bucket, castType.clone(SqlParserPos.ZERO));
    }

    /** The {@code BIGINT} bucket a logical-table GROUP BY on a raw time column becomes. */
    static SqlNode bigintBucket(String physicalTimeColumn, int step) {
        return build(new SqlIdentifier(physicalTimeColumn, SqlParserPos.ZERO), step,
                new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO), SqlParserPos.ZERO));
    }

    /** The same bucket at another step, keeping its column and CAST type. */
    static SqlNode withStep(TimeBucket bucket, int step) {
        return build(bucket.column(), step, bucket.castType());
    }
}
