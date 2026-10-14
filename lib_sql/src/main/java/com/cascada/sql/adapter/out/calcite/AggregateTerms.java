package com.cascada.sql.adapter.out.calcite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * The aggregate calls inside a select expression, in one fixed depth-first order. Canonicalization and
 * gap rewriting both number an expression's terms with this order, so term {@code k} always names
 * the same aggregate.
 */
final class AggregateTerms {

    static final Set<String> AGGREGATE_NAMES = Set.of("SUM", "COUNT", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT");

    private AggregateTerms() {
    }

    static boolean isAggregate(SqlBasicCall call) {
        return AGGREGATE_NAMES.contains(call.getOperator().getName().toUpperCase(Locale.ROOT));
    }

    /** Aggregate calls in {@code expression}; an aggregate's own arguments are not searched. */
    static List<SqlBasicCall> of(SqlNode expression) {
        List<SqlBasicCall> terms = new ArrayList<>();
        collect(expression, terms);
        return terms;
    }

    /**
     * A new expression with each aggregate term replaced by the matching column name. Calcite's
     * {@code clone} shares nested nodes, so the shuttle builds new calls and leaves the input intact.
     */
    static SqlNode replaceWithColumns(SqlNode expression, List<String> termColumns) {
        Iterator<String> columns = termColumns.iterator();
        SqlNode replaced = expression.accept(new SqlShuttle() {
            @Override
            public SqlNode visit(SqlCall call) {
                if (call instanceof SqlBasicCall basic && isAggregate(basic)) {
                    return new SqlIdentifier(columns.next(), SqlParserPos.ZERO);
                }
                return super.visit(call);
            }
        });
        if (columns.hasNext()) {
            throw new IllegalStateException("more term columns than aggregate terms");
        }
        return replaced;
    }

    private static void collect(SqlNode node, List<SqlBasicCall> terms) {
        if (node instanceof SqlBasicCall call && isAggregate(call)) {
            terms.add(call);
            return;
        }
        if (node instanceof SqlNodeList list) {
            for (SqlNode child : list) {
                collect(child, terms);
            }
        } else if (node instanceof SqlCall call) {
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    collect(operand, terms);
                }
            }
        }
    }
}
