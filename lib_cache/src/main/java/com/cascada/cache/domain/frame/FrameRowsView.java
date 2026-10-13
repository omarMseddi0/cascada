package com.cascada.cache.domain.frame;

import java.util.AbstractList;
import java.util.Map;

final class FrameRowsView extends AbstractList<Map<String, Object>> {
    private final ResultFrame frame;

    FrameRowsView(ResultFrame frame) { this.frame = frame; }

    @Override
    public Map<String, Object> get(int row) {
        if (row < 0 || row >= frame.rowCount()) {
            throw new IndexOutOfBoundsException(row);
        }
        return new FrameRowView(frame, row);
    }

    @Override
    public int size() {
        return frame.rowCount();
    }
}
