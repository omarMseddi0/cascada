package com.cascada.app.adapter.in.api;

import com.cascada.cache.domain.frame.ResultFrame;
import java.math.BigDecimal;

/**
 * One typed dispatch over a result cell for every response encoder (JSON, Parquet, protobuf rows). A
 * switch statement over the column type compiles even when a case is missing; this one fails loudly if
 * a new column type is ever added without an encoding.
 */
final class ResultCells {

    private ResultCells() {
    }

    static <E extends Exception> void visit(ResultFrame frame, int row, int column, ResultCellVisitor<E> visitor) throws E {
        if (frame.isNullAt(row, column)) {
            visitor.nullValue();
            return;
        }
        switch (frame.columnTypeAt(column)) {
            case LONG -> visitor.longValue(frame.longAt(row, column));
            case DOUBLE -> visitor.doubleValue(frame.doubleAt(row, column));
            case STRING -> visitor.stringValue(frame.stringAt(row, column));
            case DECIMAL -> visitor.decimalValue((BigDecimal) frame.valueAt(row, column));
            default -> throw new IllegalStateException("no encoding for column type " + frame.columnTypeAt(column));
        }
    }
}
