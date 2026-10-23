package com.cascada.cache.benchmark;

import com.cascada.cache.application.service.FrameMergeService;
import com.cascada.cache.benchmark.fast.FastAvgMerge;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * args: rows groups keyType(LONG|STRING) order(seq|shuffled) queries mode(base|fast) workers rounds
 *       or: check rows groups keyType order workers   (bit-exact comparison base vs fast)
 */
public final class Compare {
    static final com.sun.management.ThreadMXBean THREAD = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    static final List<String> DIMS = List.of("group_id", "region_id", "device_id");
    static final List<String> AVGS = List.of("avg_value_0", "avg_value_1", "avg_value_2");

    static HighCardinalityAvg12ColumnBenchmark.Dataset data(int rows, int groups, boolean strings, boolean shuffled, int id) {
        var d = HighCardinalityAvg12ColumnBenchmark.generate(rows, groups, 300, strings, id);
        if (!shuffled) return d;
        // Global row permutation: every bucket frame then holds groups in arbitrary order, like real
        // Spark output, instead of the generator's ascending-group order.
        int[] frameOf = new int[rows], rowOf = new int[rows];
        int index = 0;
        for (int f = 0; f < d.frames().size(); f++)
            for (int r = 0; r < d.frames().get(f).rowCount(); r++, index++) { frameOf[index] = f; rowOf[index] = r; }
        Random random = new Random(42 + id);
        for (int i = rows - 1; i > 0; i--) {
            int j = random.nextInt(i + 1);
            int t = frameOf[i]; frameOf[i] = frameOf[j]; frameOf[j] = t;
            t = rowOf[i]; rowOf[i] = rowOf[j]; rowOf[j] = t;
        }
        List<ResultFrame> out = new ArrayList<>();
        index = 0;
        for (ResultFrame shape : d.frames()) {
            ResultFrameBuilder b = ResultFrame.builder().expectedRows(shape.rowCount());
            for (String c : shape.columnNames()) b.column(c, shape.columnType(c));
            for (int r = 0; r < shape.rowCount(); r++, index++) {
                ResultFrame src = d.frames().get(frameOf[index]);
                for (int c = 0; c < shape.columnNames().size(); c++) b.appendCell(src, rowOf[index], c);
            }
            out.add(b.build());
        }
        return new HighCardinalityAvg12ColumnBenchmark.Dataset(List.copyOf(out), d.query(), d.id());
    }

    record Run(ResultFrame frame, long nanos, long acceptNanos, long cpu, long alloc, long state) {}

    static Run run(HighCardinalityAvg12ColumnBenchmark.Dataset d, String mode, int workers, ExecutorService pool) throws Exception {
        long tid = Thread.currentThread().threadId();
        long a0 = THREAD.getThreadAllocatedBytes(tid), t0 = System.nanoTime();
        ResultFrame result;
        long accepted, state = 0;
        if (mode.equals("base")) {
            var m = (workers == 1 ? new FrameMergeService(300, "ts") : new FrameMergeService(300, "ts", workers, pool)).incremental(d.query());
            for (var f : d.frames()) m.add(f);
            accepted = System.nanoTime();
            result = m.finish();
        } else {
            var m = new FastAvgMerge(DIMS, AVGS, workers, pool);
            for (var f : d.frames()) m.add(f);
            accepted = System.nanoTime();
            result = m.finish();
            state = m.stateBytes();
        }
        long t1 = System.nanoTime();
        return new Run(result, t1 - t0, accepted - t0, 0, THREAD.getThreadAllocatedBytes(tid) - a0, state);
    }

    public static void main(String[] a) throws Exception {
        THREAD.setThreadAllocatedMemoryEnabled(true);
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> { e.printStackTrace(); System.exit(2); });
        if (a[0].equals("check")) { check(a); return; }
        int rows = Integer.parseInt(a[0]), groups = Integer.parseInt(a[1]);
        boolean strings = a[2].equals("STRING"), shuffled = a[3].equals("shuffled");
        int queries = Integer.parseInt(a[4]);
        String mode = a[5];
        int workers = Integer.parseInt(a[6]), rounds = Integer.parseInt(a[7]);
        List<HighCardinalityAvg12ColumnBenchmark.Dataset> sets = new ArrayList<>();
        for (int q = 0; q < queries; q++) sets.add(data(rows, groups, strings, shuffled, q));
        ExecutorService drivers = Executors.newFixedThreadPool(queries);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        var mem = ManagementFactory.getMemoryMXBean();
        var os = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        ScheduledExecutorService sampler = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r); t.setDaemon(true); return t; });
        for (int round = -1; round < rounds; round++) {
            System.gc(); System.gc();
            long retained = mem.getHeapMemoryUsage().getUsed();
            AtomicLong peak = new AtomicLong(retained);
            var task = sampler.scheduleAtFixedRate(() -> peak.accumulateAndGet(mem.getHeapMemoryUsage().getUsed(), Math::max), 0, 5, TimeUnit.MILLISECONDS);
            long cpu0 = os.getProcessCpuTime(), t0 = System.nanoTime();
            List<Future<Run>> fs = new ArrayList<>();
            for (var d : sets) fs.add(drivers.submit(() -> run(d, mode, workers, pool)));
            List<Run> runs = new ArrayList<>();
            for (var f : fs) runs.add(f.get());
            long wall = System.nanoTime() - t0, cpu = os.getProcessCpuTime() - cpu0;
            task.cancel(false);
            for (int q = 0; q < queries; q++)
                HighCardinalityAvg12ColumnBenchmark.verify(new HighCardinalityAvg12ColumnBenchmark.Answer(runs.get(q).frame(), 0, 0, 0, 0, 0, 0, q), rows, groups, strings);
            double secs = wall / 1e9;
            System.out.printf("%s round=%d mode=%s w=%d q=%d groups=%d %s %s wall=%.3fs accept=%.3fs Mrows/s=%.2f cpu=%.1fs cores=%.2f peakHeapOverInput=%.3fGiB state/q=%.3fGiB alloc/q=%.3fGiB verify=PASS%n",
                    round < 0 ? "warmup" : "RESULT", round, mode, workers, queries, groups, a[2], a[3], secs,
                    runs.get(0).acceptNanos() / 1e9, (double) rows * queries / secs / 1e6, cpu / 1e9, cpu / (double) wall,
                    (peak.get() - retained) / 1073741824.0, runs.get(0).state() / 1073741824.0, runs.get(0).alloc() / 1073741824.0);
            runs.clear();
        }
        drivers.shutdownNow();
        System.exit(0);
    }

    static void check(String[] a) throws Exception {
        int rows = Integer.parseInt(a[1]), groups = Integer.parseInt(a[2]);
        boolean strings = a[3].equals("STRING"), shuffled = a[4].equals("shuffled");
        int workers = Integer.parseInt(a[5]);
        var d = data(rows, groups, strings, shuffled, 0);
        ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();
        ResultFrame base = run(d, "base", workers, pool).frame();
        int repeats = a.length > 6 ? Integer.parseInt(a[6]) : 1;
        for (int rep = 0; rep < repeats; rep++) {
        ResultFrame fast = run(d, "fast", workers, pool).frame();
        if (!base.columnNames().equals(fast.columnNames()) || base.rowCount() != fast.rowCount())
            throw new AssertionError("shape " + base.columnNames() + " vs " + fast.columnNames());
        for (int c = 0; c < base.columnNames().size(); c++) {
            if (base.columnTypeAt(c) != fast.columnTypeAt(c)) throw new AssertionError("type " + c);
            for (int r = 0; r < base.rowCount(); r++) {
                Object x = base.valueAt(r, c), y = fast.valueAt(r, c);
                boolean same = x == null ? y == null
                        : x instanceof Double dx ? y instanceof Double dy && Double.doubleToLongBits(dx) == Double.doubleToLongBits(dy)
                        : x.equals(y);
                if (!same) throw new AssertionError("cell r=" + r + " c=" + c + " base=" + x + " fast=" + y);
            }
        }
        System.out.println("CHECK bit-exact PASS rep=" + rep + " rows=" + rows + " groups=" + groups + " " + a[3] + " " + a[4] + " workers=" + workers + " outputRows=" + base.rowCount());
        HighCardinalityAvg12ColumnBenchmark.verify(new HighCardinalityAvg12ColumnBenchmark.Answer(fast, 0, 0, 0, 0, 0, 0, 0), rows, groups, strings);
        }
    }
}
