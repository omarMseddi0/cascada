package com.cascada.cache.domain.frame;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Fixed-size output storage with independent append-only row ranges. Each writer must have one
 * owner. Boundaries align to 64-row validity words, so null writes never race. The caller must join
 * all writers before build; completed storage is sealed and becomes an immutable ResultFrame.
 */
public final class ParallelFrameBuilder {
    private final List<String> names;
    private final Map<String, ColumnType> types;
    private final Object[] columns;
    private final long[][] present;
    private final ResultFrameBuilder[] writers;
    private final int[] bounds;
    private final int rows;
    private ResultFrame result;

    public ParallelFrameBuilder(ResultFrame schema, int rows, int writerCount) {
        if (rows < 0 || writerCount <= 0) throw new IllegalArgumentException("invalid row/writer count");
        names = schema.columnNames();
        types = schema.columnTypes();
        if (rows > 0 && names.isEmpty()) throw new IllegalArgumentException("nonempty output needs columns");
        this.rows = rows;
        columns = new Object[names.size()];
        present = new long[names.size()][];
        int words = (int) (((long) rows + 63) >>> 6);
        for (int column = 0; column < names.size(); column++) {
            columns[column] = ResultFrameBuilder.newColumn(types.get(names.get(column)), rows);
            present[column] = new long[words];
            Arrays.fill(present[column], -1L);
        }
        int count = Math.min(writerCount, words);
        writers = new ResultFrameBuilder[count];
        bounds = new int[count + 1];
        for (int index = 0; index < count; index++) {
            int start = (int) Math.min(rows, ((long) words * index / count) * 64);
            int end = (int) Math.min(rows, ((long) words * (index + 1) / count) * 64);
            bounds[index] = start;
            bounds[index + 1] = end;
            writers[index] = new ResultFrameBuilder(names, types, columns, present, start, end);
        }
    }

    public int writerCount() { return writers.length; }
    public ResultFrameBuilder writer(int index) { return writers[index]; }
    public int startRow(int index) { return bounds[index]; }
    public int endRow(int index) { return bounds[index + 1]; }

    public ResultFrame build() {
        if (result != null) return result;
        for (ResultFrameBuilder writer : writers) {
            if (!writer.rangeComplete()) throw new IllegalStateException("an output row range is incomplete");
        }
        for (ResultFrameBuilder writer : writers) writer.sealRange();
        // All-valid columns retain the normal bitmap-free representation.
        for (int column = 0; column < present.length; column++) {
            boolean allValid = true;
            for (long word : present[column]) {
                if (word != -1L) { allValid = false; break; }
            }
            if (allValid) present[column] = null;
        }
        result = new ResultFrame(names, types, columns.clone(), present.clone(), rows);
        return result;
    }
}
