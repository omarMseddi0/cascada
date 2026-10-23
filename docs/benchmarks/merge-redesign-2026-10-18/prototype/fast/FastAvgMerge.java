package com.cascada.cache.benchmark.fast;

import com.cascada.cache.application.service.merge.AggregateOperation;
import com.cascada.cache.application.service.merge.SparkAverage;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AverageIngredients;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Prototype incremental GROUP BY + AVG merge. {@code workers == 1} runs inline on the caller's
 * thread; {@code workers > 1} hash-partitions groups so each worker owns a disjoint group table
 * (no locks, no shared mutable state) and frames are fanned out to all workers.
 */
public final class FastAvgMerge {
    private static final ResultFrame END = ResultFrame.empty();
    private final List<String> dimensions;
    private final List<String> averages;
    private final int workers;
    private final ExecutorService pool;
    private GroupTable.Layout layout;
    private GroupTable[] tables;
    private List<BlockingQueue<ResultFrame>> queues;
    private List<Future<int[]>> running;
    private volatile Throwable failure;
    private long[] inlineHashes = new long[0];
    private int[] inlineGroups = new int[0];

    public FastAvgMerge(List<String> dimensions, List<String> averages, int workers, ExecutorService pool) {
        this.dimensions = List.copyOf(dimensions);
        this.averages = List.copyOf(averages);
        this.workers = workers;
        this.pool = pool;
    }

    public void add(ResultFrame frame) throws InterruptedException {
        if (layout == null) start(frame);
        if (frame.isEmpty()) return;
        if (workers == 1) {
            process(tables[0], frame, 0, 1);
            return;
        }
        for (BlockingQueue<ResultFrame> queue : queues) put(queue, frame);
    }

    /** A dead worker must fail the merge instead of leaving the producer blocked on its full queue. */
    private void put(BlockingQueue<ResultFrame> queue, ResultFrame frame) throws InterruptedException {
        while (!queue.offer(frame, 50, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            if (failure != null) throw new IllegalStateException("merge worker failed", failure);
        }
    }

    private void start(ResultFrame schema) {
        Map<String, AggregateOperation> ops = new LinkedHashMap<>();
        for (String alias : averages) {
            ops.put(AverageIngredients.sumColumn(alias), AggregateOperation.SUM);
            ops.put(AverageIngredients.countColumn(alias), AggregateOperation.COUNT);
            ops.put(AverageIngredients.typeColumn(alias), AggregateOperation.MINIMUM);
        }
        layout = new GroupTable.Layout(schema, dimensions, ops);
        schemaNames = schema.columnNames().toArray(String[]::new);
        tables = new GroupTable[workers];
        for (int w = 0; w < workers; w++) tables[w] = new GroupTable(layout);
        if (workers > 1) {
            queues = new ArrayList<>();
            running = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                // Bounded: a slow worker back-pressures the fetch instead of buffering frames.
                BlockingQueue<ResultFrame> queue = new ArrayBlockingQueue<>(8);
                queues.add(queue);
                int partition = w;
                running.add(pool.submit(() -> {
                  try {
                    long[] hashes = new long[0];
                    int[] groups = new int[0];
                    for (ResultFrame frame = queue.take(); frame != END; frame = queue.take()) {
                        if (hashes.length < frame.rowCount()) {
                            hashes = new long[frame.rowCount()];
                            groups = new int[frame.rowCount()];
                        }
                        FrameColumnReader[] in = readers(frame);
                        GroupTable.hash(in, frame.rowCount(), layout, hashes);
                        tables[partition].accept(in, frame.rowCount(), hashes, groups, partition, workers);
                    }
                    tables[partition].releaseSlots();
                    return tables[partition].sortedGroups();
                  } catch (Throwable error) {
                    failure = error;
                    error.printStackTrace();
                    throw error;
                  }
                }));
            }
        }
    }

    private void process(GroupTable table, ResultFrame frame, int partition, int partitions) {
        if (inlineHashes.length < frame.rowCount()) {
            inlineHashes = new long[frame.rowCount()];
            inlineGroups = new int[frame.rowCount()];
        }
        FrameColumnReader[] in = readers(frame);
        GroupTable.hash(in, frame.rowCount(), layout, inlineHashes);
        table.accept(in, frame.rowCount(), inlineHashes, inlineGroups, partition, partitions);
    }

    private static FrameColumnReader[] readers(ResultFrame frame) {
        FrameColumnReader[] in = new FrameColumnReader[frame.columnNames().size()];
        for (int c = 0; c < in.length; c++) in[c] = frame.columnReader(c);
        return in;
    }

    /** Sizes of the merge state right before emitting (probe tables already released). */
    public long stateBytes() {
        long bytes = 0;
        for (GroupTable table : tables) bytes += table.retainedBytes();
        return bytes;
    }

    public ResultFrame finish() throws Exception {
        int[][] orders = new int[workers][];
        if (workers == 1) {
            tables[0].releaseSlots();
            orders[0] = tables[0].sortedGroups();
        } else {
            for (BlockingQueue<ResultFrame> queue : queues) put(queue, END);
            for (int w = 0; w < workers; w++) orders[w] = running.get(w).get();
        }
        int total = 0;
        for (int[] order : orders) total += order.length;

        // Output schema: keys in place, each AVG at its SUM column, COUNT/type dropped (SparkAverage.finish).
        ResultFrameBuilder out = ResultFrame.builder().expectedRows(total);
        int keyCount = layout.keyCount;
        int[] sumMeasure = new int[averages.size()], countMeasure = new int[averages.size()];
        for (int a = 0; a < averages.size(); a++) {
            sumMeasure[a] = measureIndex(AverageIngredients.sumColumn(averages.get(a)));
            countMeasure[a] = measureIndex(AverageIngredients.countColumn(averages.get(a)));
            if (layout.measureTypes[sumMeasure[a]] != ColumnType.DOUBLE)
                throw new UnsupportedOperationException("prototype: only DOUBLE AVG");
        }
        for (int k = 0; k < keyCount; k++) out.column(dimensions.get(k), layout.keyTypes[k]);
        for (String alias : averages) out.column(alias, ColumnType.DOUBLE);

        // k-way merge of the per-partition sorted runs (partitions are disjoint, so no ties).
        int[] cursor = new int[workers];
        for (int emitted = 0; emitted < total; emitted++) {
            int best = -1;
            for (int w = 0; w < workers; w++) {
                if (cursor[w] == orders[w].length) continue;
                if (best < 0 || compareAcross(w, orders[w][cursor[w]], best, orders[best][cursor[best]]) < 0) best = w;
            }
            GroupTable table = tables[best];
            int group = orders[best][cursor[best]++];
            for (int k = 0; k < keyCount; k++) {
                if (!table.flag(group, k)) { out.appendNull(); continue; }
                switch (layout.keyTypes[k]) {
                    case LONG -> out.appendLong(table.word(group, layout.keyOffset[k]));
                    case DOUBLE -> out.appendDouble(Double.longBitsToDouble(table.word(group, layout.keyOffset[k])));
                    case STRING -> out.appendString(table.stringKey(group, k));
                    case DECIMAL -> throw new UnsupportedOperationException();
                }
            }
            for (int a = 0; a < averages.size(); a++) {
                int c = countMeasure[a], s = sumMeasure[a];
                long count = table.flag(group, layout.measureFlag[c])
                        ? SparkAverage.exactCount(table.word(group, layout.measureOffset[c])) : 0L;
                if (count == 0 || !table.flag(group, layout.measureFlag[s])) out.appendNull();
                else out.appendDouble(Double.longBitsToDouble(table.word(group, layout.measureOffset[s])) / count);
            }
        }
        return out.build();
    }

    private int compareAcross(int leftTable, int left, int rightTable, int right) {
        if (leftTable == rightTable) return tables[leftTable].compare(left, right);
        // Compare through a key-wise view across tables.
        GroupTable a = tables[leftTable], b = tables[rightTable];
        for (int k = 0; k < layout.keyCount; k++) {
            boolean pa = a.flag(left, k), pb = b.flag(right, k);
            int c;
            if (!pa || !pb) c = pa == pb ? 0 : pa ? 1 : -1;
            else c = switch (layout.keyTypes[k]) {
                case LONG -> Long.compare(a.word(left, layout.keyOffset[k]), b.word(right, layout.keyOffset[k]));
                case DOUBLE -> Double.compare(Double.longBitsToDouble(a.word(left, layout.keyOffset[k])),
                        Double.longBitsToDouble(b.word(right, layout.keyOffset[k])));
                case STRING -> com.cascada.cache.application.service.merge.SparkValues.compareStrings(
                        a.stringKey(left, k), b.stringKey(right, k));
                case DECIMAL -> 0;
            };
            if (c != 0) return c;
        }
        return 0;
    }

    private int measureIndex(String column) {
        for (int m = 0; m < layout.measureCount; m++)
            if (layout.measureColumns[m] >= 0 && columnName(layout.measureColumns[m]).equals(column)) return m;
        throw new IllegalArgumentException("missing " + column);
    }

    private String schemaNames[];

    private String columnName(int column) {
        return schemaNames[column];
    }
}
