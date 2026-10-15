package com.cascada.sql.adapter.calcite;

import com.cascada.sql.domain.UnsupportedSqlException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;

/** Reads the integer literal forms used for SQL limits, epoch bounds, and bucket widths. */
final class SqlNumericLiteralValue {

    private SqlNumericLiteralValue() {
    }

    static Long asLong(SqlNode node) {
        if (!(node instanceof SqlNumericLiteral literal) || !literal.isInteger()) {
            return null;
        }
        try {
            return literal.getValueAs(Long.class);
        } catch (ArithmeticException overflow) {
            throw new UnsupportedSqlException("integer literal is outside the signed 64-bit range", overflow);
        }
    }
}
