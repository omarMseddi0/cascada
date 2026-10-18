package com.cascada.sql.adapter.out.calcite;

import com.cascada.sql.adapter.out.calcite.model.TimeBucket;
import com.cascada.sql.domain.model.TimeDimensionMap;
import java.util.Optional;
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
            if (TimeBuckets.match(expression, dimensions).isPresent())
                return alias.getOperator().createCall(alias.getParserPosition(), normalize(expression, dimensions, step), alias.operand(1));
            return node;
        }
        return TimeBuckets.match(node, dimensions).isPresent() ? normalize(node, dimensions, step) : node;
    }

    static SqlNode normalize(SqlNode node, TimeDimensionMap dimensions, int step) {
        return node.accept(new SqlShuttle() {
            @Override public SqlNode visit(SqlCall call) {
                Optional<TimeBucket> bucket = TimeBuckets.match(call, dimensions);
                return bucket.isPresent() ? TimeBuckets.withStep(bucket.get(), step) : super.visit(call);
            }
        });
    }
}
