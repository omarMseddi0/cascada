package com.cascada.cache.application.service;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * A bucket-labelled gap result split into the frame each per-bucket gap query would have returned.
 * The label column is removed, so every frame has the schema of an ordinary bucket ingredient.
 */
final class BucketSplit {

    private final ResultFrame emptyFrame;
    private final Map<Long, ResultFrame> frames;

    private BucketSplit(ResultFrame emptyFrame, Map<Long, ResultFrame> frames) {
        this.emptyFrame = emptyFrame;
        this.frames = frames;
    }

    static BucketSplit of(ResultFrame labelled, String bucketColumn) {
        int label = labelled.columnIndex(bucketColumn);
        if (labelled.columnTypeAt(label) != ColumnType.LONG) {
            throw new IllegalStateException("bucket label column '" + bucketColumn + "' must be BIGINT");
        }
        int columnCount = labelled.columnNames().size();
        int[] kept = new int[columnCount - 1];
        int next = 0;
        for (int column = 0; column < columnCount; column++) {
            if (column != label) {
                kept[next] = column;
                next++;
            }
        }

        FrameColumnReader labels = labelled.columnReader(label);
        Map<Long, Integer> rowCounts = new HashMap<>();
        for (int row = 0; row < labelled.rowCount(); row++) {
            if (labels.isNullAt(row)) {
                throw new IllegalStateException("a gap row has no bucket label");
            }
            rowCounts.merge(labels.longValue(row), 1, Integer::sum);
        }

        Map<Long, ResultFrameBuilder> builders = new LinkedHashMap<>();
        for (int row = 0; row < labelled.rowCount(); row++) {
            long bucket = labels.longValue(row);
            ResultFrameBuilder builder = builders.computeIfAbsent(bucket,
                    ignored -> schemaBuilder(labelled, kept).expectedRows(rowCounts.get(bucket)));
            for (int column : kept) {
                builder.appendCell(labelled, row, column);
            }
        }
        Map<Long, ResultFrame> frames = new LinkedHashMap<>();
        builders.forEach((bucket, builder) -> frames.put(bucket, builder.build()));
        return new BucketSplit(schemaBuilder(labelled, kept).build(), Collections.unmodifiableMap(frames));
    }

    /** The bucket's rows, or a zero-row frame with the ingredient schema when Spark returned none. */
    ResultFrame frameFor(long bucketStart) {
        return frames.getOrDefault(bucketStart, emptyFrame);
    }

    boolean hasRowsFor(long bucketStart) {
        return frames.containsKey(bucketStart);
    }

    Set<Long> bucketStarts() {
        return frames.keySet();
    }

    private static ResultFrameBuilder schemaBuilder(ResultFrame labelled, int[] kept) {
        ResultFrameBuilder builder = ResultFrame.builder();
        for (int column : kept) {
            builder.column(labelled.columnNames().get(column), labelled.columnTypeAt(column));
        }
        return builder;
    }
}
