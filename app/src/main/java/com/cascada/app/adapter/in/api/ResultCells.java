package com.cascada.app.adapter.in.api;

import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import java.util.Arrays;

/** Binds typed column readers once and dispatches cells for the HTTP and gRPC encoders. */
final class ResultCells {

    private ResultCells() {
    }

    static FrameColumnReader[] readers(ResultFrame frame) {
        FrameColumnReader[] readers = new FrameColumnReader[frame.columnNames().size()];
        Arrays.setAll(readers, frame::columnReader);
        return readers;
    }

    static <E extends Exception> void visit(FrameColumnReader column, int row, ResultCellVisitor<E> visitor)
            throws E {
        if (column.isNullAt(row)) {
            visitor.nullValue();
            return;
        }
        switch (column.type()) {
            case LONG -> visitor.longValue(column.longValue(row));
            case DOUBLE -> visitor.doubleValue(column.doubleValue(row));
            case STRING -> visitor.stringValue(column.stringValue(row));
            case DECIMAL -> visitor.decimalValue(column.decimalValue(row));
        }
    }
}
