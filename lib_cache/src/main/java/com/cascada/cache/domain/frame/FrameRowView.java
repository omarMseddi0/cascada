package com.cascada.cache.domain.frame;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Set;

final class FrameRowView extends AbstractMap<String, Object> {
    private final ResultFrame frame;
    private final int row;

    FrameRowView(ResultFrame frame, int row) {
        this.frame = frame;
        this.row = row;
    }

    @Override
    public Object get(Object key) {
        return frame.rowValue(row, key);
    }

    @Override
    public boolean containsKey(Object key) {
        return frame.columnTypes().containsKey(key);
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>(frame.columnNames().size());
        for (int column = 0; column < frame.columnNames().size(); column++) {
            values.put(frame.columnNames().get(column), frame.valueAt(row, column));
        }
        return values.entrySet();
    }
}
