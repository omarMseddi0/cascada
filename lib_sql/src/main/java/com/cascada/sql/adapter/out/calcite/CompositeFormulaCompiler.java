package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.query.ArithmeticOperator;
import com.cascada.cache.domain.model.query.CompositeFormula;
import com.cascada.cache.domain.model.query.FormulaBinary;
import com.cascada.cache.domain.model.query.FormulaColumn;
import com.cascada.cache.domain.model.query.FormulaLiteral;
import com.cascada.sql.exception.UnsupportedSqlException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

/** Compiles the already parsed and ingredient-rewritten Calcite expression into a cache domain plan. */
final class CompositeFormulaCompiler {
    private CompositeFormulaCompiler() {}

    static CompositeFormula compile(SqlNode node) {
        if (node instanceof SqlIdentifier id && id.isSimple()) return new FormulaColumn(id.getSimple());
        if (node instanceof SqlNumericLiteral literal) {
            try { return new FormulaLiteral(literal.bigDecimalValue().longValueExact()); }
            catch (ArithmeticException failure) { throw new UnsupportedSqlException("non-integral composite literal"); }
        }
        if (node instanceof SqlCall call) {
            if (call.operandCount() == 1) return switch (call.getKind()) {
                case PLUS_PREFIX -> compile(call.operand(0));
                case MINUS_PREFIX -> new FormulaBinary(ArithmeticOperator.SUBTRACT, new FormulaLiteral(0), compile(call.operand(0)));
                default -> throw new UnsupportedSqlException("unsupported composite unary operation");
            };
            if (call.operandCount() == 2) {
                ArithmeticOperator operator = switch (call.getKind()) {
                    case PLUS -> ArithmeticOperator.ADD;
                    case MINUS -> ArithmeticOperator.SUBTRACT;
                    case TIMES -> ArithmeticOperator.MULTIPLY;
                    case DIVIDE -> ArithmeticOperator.DIVIDE;
                    default -> throw new UnsupportedSqlException("unsupported composite operation");
                };
                return new FormulaBinary(operator, compile(call.operand(0)), compile(call.operand(1)));
            }
        }
        throw new UnsupportedSqlException("unsupported composite operand");
    }
}
