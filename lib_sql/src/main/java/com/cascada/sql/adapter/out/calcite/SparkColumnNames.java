package com.cascada.sql.adapter.out.calcite;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

/**
 * The column name Spark gives an unaliased select item, for the shapes the cache rebuilds itself:
 * aggregate calls over columns ({@code avg(latency)}, {@code count(1)} for {@code COUNT(*)}), numeric
 * literals, unary minus and the four arithmetic operators ({@code (sum(a) / count(b))}). Other
 * shapes have no reproducible name and must be aliased to be cached.
 */
final class SparkColumnNames {

    private SparkColumnNames() {
    }

    static Optional<String> autoName(SqlNode expression) {
        if (expression instanceof SqlIdentifier identifier) {
            return Optional.of(identifier.names.get(identifier.names.size() - 1));
        }
        if (expression instanceof SqlNumericLiteral literal) {
            return Optional.of(literal.toValue());
        }
        if (!(expression instanceof SqlBasicCall call)) {
            return Optional.empty();
        }
        if (AggregateTerms.isAggregate(call)) {
            return aggregateName(call);
        }
        String operator = switch (call.getKind()) {
            case PLUS -> "+";
            case MINUS -> "-";
            case TIMES -> "*";
            case DIVIDE -> "/";
            default -> null;
        };
        if (operator != null && call.operandCount() == 2) {
            Optional<String> left = autoName(call.operand(0));
            Optional<String> right = autoName(call.operand(1));
            if (left.isEmpty() || right.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of("(" + left.get() + " " + operator + " " + right.get() + ")");
        }
        if (call.getKind() == SqlKind.MINUS_PREFIX) {
            return autoName(call.operand(0)).map(operand -> "(- " + operand + ")");
        }
        return Optional.empty();
    }

    private static Optional<String> aggregateName(SqlBasicCall call) {
        String function = call.getOperator().getName().toLowerCase(Locale.ROOT);
        if (call.getFunctionQuantifier() != null) {
            return Optional.empty();
        }
        List<String> arguments = new ArrayList<>();
        for (SqlNode operand : call.getOperandList()) {
            if (operand instanceof SqlIdentifier star && star.isStar()) {
                arguments.add("1");
                continue;
            }
            if (!(operand instanceof SqlIdentifier) && !(operand instanceof SqlLiteral)) {
                return Optional.empty();
            }
            Optional<String> argument = autoName(operand);
            if (argument.isEmpty()) {
                return Optional.empty();
            }
            arguments.add(argument.get());
        }
        return Optional.of(function + "(" + String.join(", ", arguments) + ")");
    }
}
