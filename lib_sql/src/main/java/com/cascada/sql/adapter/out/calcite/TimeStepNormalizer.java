package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.domain.TimeDimensionMap;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

/** Rewrites only recognized epoch time buckets; ordinary arithmetic literals are untouched. */
final class TimeStepNormalizer {
    private TimeStepNormalizer() { }

    static SqlNode bucketOutput(SqlNode node, TimeDimensionMap dimensions, int step) {
        if (node.getKind() == SqlKind.AS && node instanceof SqlBasicCall alias) {
            SqlNode expression = alias.operand(0);
            if (CanonicalTimeSeriesAnalyzer.floorBucketStep(expression, dimensions).isPresent())
                return alias.getOperator().createCall(alias.getParserPosition(), normalize(expression, dimensions, step), alias.operand(1));
            return node;
        }
        return CanonicalTimeSeriesAnalyzer.floorBucketStep(node, dimensions).isPresent()
                ? normalize(node, dimensions, step) : node;
    }

    static SqlNode normalize(SqlNode node, TimeDimensionMap dimensions, int step) {
        return node.accept(new SqlShuttle() {
            @Override public SqlNode visit(SqlCall call) {
                if (CanonicalTimeSeriesAnalyzer.floorBucketStep(call, dimensions).isPresent()) {
                    SqlNode expression = call.getKind() == SqlKind.CAST ? ((SqlBasicCall) call).operand(0) : call;
                    SqlBasicCall times = (SqlBasicCall) expression;
                    SqlBasicCall floor = (SqlBasicCall) (times.operand(0).getKind() == SqlKind.FLOOR
                            ? times.operand(0) : times.operand(1));
                    SqlNode column = ((SqlBasicCall) floor.operand(0)).operand(0);
                    String bucket = "FLOOR(" + CalciteSql.unparse(column) + " / " + step + ") * " + step;
                    return CalciteSql.parseExpression(call.getKind() == SqlKind.CAST
                            ? "CAST(" + bucket + " AS BIGINT)" : bucket);
                }
                return super.visit(call);
            }
        });
    }
}
