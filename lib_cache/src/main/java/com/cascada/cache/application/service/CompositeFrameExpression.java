package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.query.ArithmeticOperator;
import com.cascada.cache.domain.query.CompositeFormula;
import com.cascada.cache.domain.query.FormulaBinary;
import com.cascada.cache.domain.query.FormulaColumn;
import com.cascada.cache.domain.query.FormulaLiteral;

/** Binds a SQL-adapter-compiled arithmetic plan once to a frame schema. Does not parse SQL. */
final class CompositeFrameExpression {
    private CompositeFrameExpression() {}

    static CompositeExpressionNode bind(CompositeFormula formula, ResultFrame schema) {
        return switch (formula) {
            case FormulaLiteral literal -> literal(literal.value());
            case FormulaBinary binary -> binary(binary.operator(), bind(binary.left(), schema), bind(binary.right(), schema));
            case FormulaColumn column -> column(column.name(), schema);
        };
    }

    private static CompositeExpressionNode column(String name, ResultFrame schema) {
        int column = -1;
        for (int index = 0; index < schema.columnNames().size(); index++) {
            if (schema.columnNames().get(index).equalsIgnoreCase(name)) {
                if (column >= 0) throw new IllegalArgumentException("ambiguous composite column " + name);
                column = index;
            }
        }
        if (column < 0) throw new IllegalArgumentException("missing composite ingredient " + name);
        int resolved = column;
        ColumnType type = schema.columnTypeAt(column);
        if (type == ColumnType.STRING) throw new IllegalArgumentException("non-numeric composite operand");
        return new CompositeExpressionNode() {
            public ColumnType type() { return type; }
            public Number value(ResultFrame frame, int row) {
                return frame.isNullAt(row, resolved) ? null : (Number) frame.valueAt(row, resolved);
            }
        };
    }
    private static CompositeExpressionNode literal(long value) {
        return new CompositeExpressionNode() {
            public ColumnType type() { return ColumnType.LONG; }
            public Number value(ResultFrame frame, int row) { return value; }
        };
    }
    private static CompositeExpressionNode binary(ArithmeticOperator operator, CompositeExpressionNode left, CompositeExpressionNode right) {
        ColumnType type = left.type() == ColumnType.DECIMAL || right.type() == ColumnType.DECIMAL
                ? ColumnType.DECIMAL : operator == ArithmeticOperator.DIVIDE || left.type() == ColumnType.DOUBLE || right.type() == ColumnType.DOUBLE
                ? ColumnType.DOUBLE : ColumnType.LONG;
        if (type == ColumnType.DECIMAL)
            throw new IllegalArgumentException("decimal composite requires Spark precision/scale evaluation");
        return new CompositeExpressionNode() {
            public ColumnType type() { return type; }
            public Number value(ResultFrame frame, int row) {
                Number a = left.value(frame, row), b = right.value(frame, row);
                if (a == null || b == null || operator == ArithmeticOperator.DIVIDE && b.doubleValue() == 0) return null;
                if (type == ColumnType.LONG) return switch (operator) {
                    case ADD -> a.longValue() + b.longValue();
                    case SUBTRACT -> a.longValue() - b.longValue();
                    case MULTIPLY -> a.longValue() * b.longValue();
                    default -> throw new IllegalArgumentException("unsupported integer operator");
                };
                return switch (operator) {
                    case ADD -> a.doubleValue() + b.doubleValue();
                    case SUBTRACT -> a.doubleValue() - b.doubleValue();
                    case MULTIPLY -> a.doubleValue() * b.doubleValue();
                    case DIVIDE -> a.doubleValue() / b.doubleValue();
                    default -> throw new IllegalArgumentException("unsupported numeric operator");
                };
            }
        };
    }
}
