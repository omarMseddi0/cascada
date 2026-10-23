# Cascada

A lakehouse query engine with a smart bucket cache

## Direct CPU occupancy checks: 23 October 2026

These tests answer whether configured threads actually run on CPUs. They instrument unchanged production revision `33e9c4b` using [Java 21 ThreadMXBean platform-thread CPU counters](https://docs.oracle.com/en/java/javase/21/docs/api/java.management/java/lang/management/ThreadMXBean.html). Each worker's occupancy is its CPU-time delta divided by merge wall time. The sum is busy-core equivalents: four workers continuously on CPU would total 4.0. These are interval averages, not a guarantee of 100% in every sampling window. They exclude caller, GC, compiler, and profiler CPU; they are not whole-process CPU utilization.

The input and independent oracle match the compact-state experiment above: 20 million globally shuffled rows, one million groups, three weighted AVG outputs, NULLs, and exactly 12 columns. Input generation, validation, and explicit GC stay outside timing. The four-worker/300-frame LONG cell has three warmups and five measured rounds with JFR profiling enabled. Each additional cell has two warmups and three measured rounds without JFR. Different frame/key shapes are separate cells, not a controlled before/after optimization comparison.

| Keys | Frames | Workers | Verified rounds | Wall seconds | Per-worker median CPU occupancy | Median busy-core equivalents |
|---|---:|---:|---:|---:|---|---:|
| LONG | 300 | 4 | 5 | 2.160 | 94.9%, 96.5%, 96.4%, 94.2% | 3.80 |
| LONG | 20 | 4 | 3 | 2.174 | 96.1%, 97.6%, 97.0%, 96.8% | 3.88 |
| STRING | 300 | 4 | 3 | 2.976 | 97.5%, 98.5%, 94.7%, 97.0% | 3.87 |
| LONG | 300 | 8 | 3 | 1.150 | 96.8%, 96.1%, 97.5%, 95.4%, 96.8%, 97.9%, 96.8%, 97.5% | 7.72 |

All 14 measured rounds passed: **280 million row visits, 14 million output groups, and 42 million checked averages**. LONG/300-frame aggregation acceptance averaged 3.86 busy-core equivalents at four workers, with finishing at 3.70. At eight workers those stages reached 7.84 and 7.48 respectively. Stage values include their scheduling/coordination time. The caller also performs routing for small frames, so configuring four workers is not a hard four-core limit for the whole JVM.

Platform threads can migrate between logical processors; these counters establish CPU work and concurrency, not affinity to four distinct physical cores. This host has eight physical cores and sixteen logical processors. No CPU affinity or host power settings were changed. No hardware performance counters were collected, so cache-miss, memory-bandwidth, NUMA, and SIMD-efficiency claims would require further measurement. An idle worker is not the same as an executing worker stalled on memory; high occupancy alone does not prove efficient execution.

### Profiler findings and hardware-level choices

The JFR profile contains 6,181 top-frame execution samples on merge workers across warmup and measured merges. `accumulateRow` accounts for 31.5%, `sameDimensions` 28.4%, `findOrCreate` 21.2%, and stable sorting 8.3%. Samples are an approximate attribution, not exact phase timing. There is no evidence here that adding a busy-spin queue would improve throughput; the four workers already spend most of the merge interval on CPU. The next controlled optimization candidates are typed update loops and key comparison/probing. Any change must preserve NULL handling, overflow behavior, cancellation, and floating-point update order, then win an equal-input comparison.

The existing representation already uses primitive pages, bit-packed flags/hash storage, hash tags, exclusive group ownership, stable parallel routing, and disjoint output ranges. [Agrona](https://github.com/aeron-io/agrona) offers primitive collections and direct buffers, but replacing this composite-key table requires measured benefits. [FFM was finalized for JDK 22](https://bugs.openjdk.org/browse/JDK-8312523); this project targets Java 21. Native memory or native calls do not by themselves fix arithmetic/probing hotspots, and would add runtime/lifetime considerations. No FFM migration or new production dependency was introduced merely to increase CPU percentage. [DuckDB's tuning guide](https://www.duckdb.org/docs/current/guides/performance/how_to_tune_workloads) likewise recommends measuring workload-specific parallelism and warns that additional threads can slow execution.

### Reproduce occupancy and JFR profiling

The complete instrumentation harness follows. Save it as `CoreUtilizationBenchmark.java`, compile as for `CompactMergeBenchmark` above, and select the same classpath and heap. Its JSON reports caller first, followed by workers, for total merge, acceptance, and finishing CPU deltas. Enable thread CPU measurement before warmups; worker threads are prestarted and remain alive while counters are read. Run cells sequentially. Local raw JSON, the recording, exported JFR events, and `SUMMARY.json` are retained under ignored `docs/benchmarks/core-utilization-2026-10-23/`.

```powershell
javac -cp $classes -d evidence CoreUtilizationBenchmark.java
java -Xms5g -Xmx5g -XX:+UseParallelGC -XX:StartFlightRecording=filename=four-workers.jfr,settings=profile,dumponexit=true -cp "evidence;$classes" com.cascada.cache.benchmark.CoreUtilizationBenchmark 20000000 1000000 300 LONG 4 3 5 four-workers.json
# Without recording: LONG/20 frames/4 workers, STRING/300/4, LONG/300/8;
# use two warmups and three measured rounds for each additional cell.
jfr print --json --events jdk.ExecutionSample,jdk.ThreadPark,jdk.JavaMonitorEnter four-workers.jfr
```

<details><summary>CoreUtilizationBenchmark.java</summary>

```java
package com.cascada.cache.benchmark;

import com.cascada.cache.application.service.FrameMergeService;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.merge.*;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.*;
import com.cascada.cache.domain.model.time.TimeRange;
import java.lang.management.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Local reproducible harness: real incremental merge and typed AVG; no Spark/network in timer. */
public final class CoreUtilizationBenchmark {
    static final String ALIAS = "avg_value";
    static final com.sun.management.ThreadMXBean THREAD = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    static final com.sun.management.OperatingSystemMXBean OS = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();
    record Dataset(List<ResultFrame> frames, CanonicalQueryObject query, int id) {}
    record Answer(ResultFrame frame, Object owner, int id) {}
    static FrameMergeService service;
    static volatile Object retainedAnswer;
    static long[] allocationThreads;
    static long allocated() { long sum=0;for(long n:THREAD.getThreadAllocatedBytes(allocationThreads))sum+=Math.max(0,n);return sum; }
    static long[] cpuSnapshot() { long[] a=new long[allocationThreads.length];for(int i=0;i<a.length;i++)a[i]=THREAD.getThreadCpuTime(allocationThreads[i]);return a; }
    static List<Double> cpuDelta(long[] start,long[] end) {var out=new ArrayList<Double>();for(int i=0;i<start.length;i++)out.add((end[i]-start[i])/1e9);return out;}
    static long used() { return MEMORY.getHeapMemoryUsage().getUsed(); }
    static void fullGc() throws Exception { System.gc();Thread.sleep(30);System.gc();Thread.sleep(30); }
    static String json(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        if (v instanceof Map<?,?> m) return "{" + String.join(",", m.entrySet().stream().map(e -> json(e.getKey().toString()) + ":" + json(e.getValue())).toList()) + "}";
        if (v instanceof Collection<?> c) return "[" + String.join(",", c.stream().map(CoreUtilizationBenchmark::json).toList()) + "]";
        return "\"" + v.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    static long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(x -> Math.max(0,x.getCollectionCount())).sum(); }
    static long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(x -> Math.max(0,x.getCollectionTime())).sum(); }
    static double base(int group, int query) { return (group % 1000) * 0.125 + query * 3.5; }
    static boolean empty(int group, int occurrence) { return group % 9973 == 0 || group % 97 == 0 && occurrence % 3 == 0; }
    static Dataset generate(int rows, int groups, int buckets, boolean strings, int id) {
        String[] keys = strings ? new String[groups] : null;
        if(strings)for(int g=0;g<groups;g++)keys[g]="q"+id+"-g"+g;
        int[] permutation=new int[rows];for(int row=0;row<rows;row++)permutation[row]=row;
        Random random=new Random(777L);
        for(int row=rows-1;row>0;row--){int at=random.nextInt(row+1),temp=permutation[row];permutation[row]=permutation[at];permutation[at]=temp;}
        List<ResultFrame> frames=new ArrayList<>(buckets);int index=0;
        for(int bucket=0;bucket<buckets;bucket++) {
            int size=rows/buckets+(bucket<rows%buckets?1:0);
            var b=ResultFrame.builder().expectedRows(size).column("group_id",strings?ColumnType.STRING:ColumnType.LONG)
                    .column("region_id",ColumnType.LONG).column("device_id",ColumnType.LONG);
            for(int metric=0;metric<3;metric++) {
                String alias="avg_value_"+metric;
                b.column(AverageIngredients.sumColumn(alias),ColumnType.DOUBLE)
                 .column(AverageIngredients.countColumn(alias),ColumnType.LONG)
                 .column(AverageIngredients.typeColumn(alias),ColumnType.STRING);
            }
            for(int j=0;j<size;j++,index++) {
                int ordinal=permutation[index],g=ordinal%groups,occurrence=ordinal/groups;
                if(strings)b.appendString(keys[g]);else b.appendLong(g+(long)id*groups);
                b.appendLong(g%101).appendLong(g%17);
                for(int metric=0;metric<3;metric++) {
                    long count=empty(g+metric*11,occurrence+metric)?0:(occurrence+metric)%5+1;
                    if(count==0)b.appendNull();else b.appendDouble((base(g,id)+metric*11.25+occurrence*(metric+1))*count);
                    b.appendLong(count).appendString("double");
                }
            }
            var frame=b.build();if(frame.columnNames().size()!=12)throw new AssertionError("input must have exactly 12 columns");frames.add(frame);
        }
        if(index!=rows)throw new AssertionError("incorrect input row count");
        var measures=new LinkedHashMap<String,AggregateFunction>();var specs=new ArrayList<String>();var output=new ArrayList<String>(List.of("group_id","region_id","device_id"));
        for(int metric=0;metric<3;metric++){String alias="avg_value_"+metric;measures.put(alias,AggregateFunction.AVERAGE);specs.add("AVG(value_"+metric+") AS "+alias);output.add(alias);}
        var metadata=QueryMetadata.globalAggregate().withAggregateSpecs(specs).withMeasureAggregates(measures);
        var query=new CanonicalQueryObject(HashComponents.of(List.of("group_id","region_id","device_id"),specs,List.of()),
                new TimeRange(0,buckets*7200L-1),PostProcessing.none(),metadata,
                "SELECT group_id,region_id,device_id,AVG(value_0),AVG(value_1),AVG(value_2) FROM benchmark GROUP BY group_id,region_id,device_id",List.of("benchmark"),List.of(),List.of(),output);
        return new Dataset(List.copyOf(frames),query,id);
    }
    static Map<String,Object> verify(Answer a,int rows,int groups,boolean strings) {
        var frame=a.frame();var expectedColumns=List.of("group_id","region_id","device_id","avg_value_0","avg_value_1","avg_value_2");
        if(frame.rowCount()!=groups||!frame.columnNames().equals(expectedColumns))throw new AssertionError("wrong shape "+frame.columnNames());
        int key=frame.columnIndex("group_id"),region=frame.columnIndex("region_id"),device=frame.columnIndex("device_id"),nulls=0;
        int[] averages=new int[3];for(int m=0;m<3;m++){averages[m]=frame.columnIndex("avg_value_"+m);if(frame.columnType("avg_value_"+m)!=ColumnType.DOUBLE)throw new AssertionError("wrong AVG type");}
        BitSet seen=new BitSet(groups);double checksum=0,maxError=0;
        for(int r=0;r<groups;r++) {
            int g;
            if(strings){String v=frame.stringAt(r,key);if(!v.startsWith("q"+a.id()+"-g"))throw new AssertionError("mixed query");g=Integer.parseInt(v.substring(v.indexOf("-g")+2));}
            else g=Math.toIntExact(frame.longAt(r,key)-(long)a.id()*groups);
            if(g<0||g>=groups||seen.get(g))throw new AssertionError("duplicate/missing key");seen.set(g);
            if(frame.longAt(r,region)!=g%101||frame.longAt(r,device)!=g%17)throw new AssertionError("corrupt composite key");
            for(int metric=0;metric<3;metric++) {
                long count=0,weighted=0;int occurrences=(rows-1-g)/groups+1;
                for(int occurrence=0;occurrence<occurrences;occurrence++)if(!empty(g+metric*11,occurrence+metric)){long n=(occurrence+metric)%5+1;count+=n;weighted+=n*occurrence;}
                int avg=averages[metric];
                if(count==0){if(!frame.isNullAt(r,avg))throw new AssertionError("all-null AVG");nulls++;}
                else {
                    if(frame.isNullAt(r,avg))throw new AssertionError("missing AVG");
                    double expected=base(g,a.id())+metric*11.25+(metric+1)*(double)weighted/count,actual=frame.doubleAt(r,avg),error=Math.abs(expected-actual);
                    if(error>1e-10*Math.max(1,Math.abs(expected)))throw new AssertionError("wrong weighted AVG group="+g+" metric="+metric+" actual="+actual+" expected="+expected);
                    checksum+=actual;maxError=Math.max(maxError,error);
                }
            }
        }
        if(seen.cardinality()!=groups)throw new AssertionError("missing groups");
        return Map.of("checkedGroups",groups,"checkedAverages",(long)groups*3,"nullAverages",nulls,"maxAbsoluteError",maxError,"checksum",checksum);
    }

    static Map<String,Object> trial(Dataset dataset, int trial, int rows, int groups, boolean strings) throws Exception {
        retainedAnswer=null;fullGc();
        long retained=used();AtomicLong peak=new AtomicLong(retained);
        var sampling=new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread sampler=Thread.ofPlatform().daemon().start(()->{
            while(sampling.get()){peak.accumulateAndGet(used(),Math::max);try{Thread.sleep(5);}catch(InterruptedException e){return;}}
        });
        long[] threads0=cpuSnapshot();long allocated0=allocated(),cpu0=OS.getProcessCpuTime(),begin=System.nanoTime();
        var merge=service.incremental(dataset.query());
        for(ResultFrame frame:dataset.frames())merge.add(frame);
        long accepted=System.nanoTime();long[] threadsAccepted=cpuSnapshot();ResultFrame result=merge.finish();long ended=System.nanoTime();long[] threadsEnd=cpuSnapshot();
        long cpu=OS.getProcessCpuTime()-cpu0,allocation=allocated()-allocated0;
        peak.accumulateAndGet(used(),Math::max);sampling.set(false);sampler.join();
        Answer answer=new Answer(result,merge,dataset.id());
        Map<String,Object> correctness=verify(answer,rows,groups,strings);
        retainedAnswer=answer;fullGc();long live=used()-retained;
        var metrics=new LinkedHashMap<String,Object>();
        metrics.put("threadCpuSecondsCallerThenWorkers",cpuDelta(threads0,threadsEnd));metrics.put("acceptThreadCpuSeconds",cpuDelta(threads0,threadsAccepted));metrics.put("finishThreadCpuSeconds",cpuDelta(threadsAccepted,threadsEnd));
        metrics.put("trial",trial);metrics.put("warmup",trial<0);
        metrics.put("seconds",(ended-begin)/1e9);metrics.put("acceptSeconds",(accepted-begin)/1e9);
        metrics.put("finishSeconds",(ended-accepted)/1e9);metrics.put("cpuSeconds",cpu/1e9);
        metrics.put("engineAllocatedBytes",allocation);metrics.put("sampledExtraHeapBytes",peak.get()-retained);
        metrics.put("liveMergeAndOutputHeapBytes",live);metrics.put("correctness",correctness);
        retainedAnswer=null;
        return metrics;
    }

    public static void main(String[] args) throws Exception {
        int rows=Integer.parseInt(args[0]),groups=Integer.parseInt(args[1]),frames=Integer.parseInt(args[2]);
        boolean strings=args[3].equals("STRING");int workers=Integer.parseInt(args[4]);
        int warmups=Integer.parseInt(args[5]),rounds=Integer.parseInt(args[6]);Path output=Path.of(args[7]);
        THREAD.setThreadAllocatedMemoryEnabled(true);THREAD.setThreadCpuTimeEnabled(true);
        var ids=new ArrayList<Long>();ids.add(Thread.currentThread().threadId());
        ThreadFactory factory=r->{Thread t=Thread.ofPlatform().name("merge-worker-"+ids.size()).unstarted(r);ids.add(t.threadId());return t;};
        var executor=new ThreadPoolExecutor(workers,workers,0,TimeUnit.SECONDS,new LinkedBlockingQueue<>(),factory);
        executor.prestartAllCoreThreads();allocationThreads=ids.stream().mapToLong(Long::longValue).toArray();
        try {
            try {service=FrameMergeService.class.getConstructor(int.class,String.class,int.class,Executor.class)
                    .newInstance(300,"ts",workers,executor);}
            catch(NoSuchMethodException olderMain){if(workers!=1)throw new IllegalArgumentException("main supports one worker");service=new FrameMergeService(300,"ts");}
            Dataset dataset=generate(rows,groups,frames,strings,0);
            var results=new ArrayList<Map<String,Object>>();
            for(int round=-warmups;round<rounds;round++) {
                Map<String,Object> metrics=trial(dataset,round,rows,groups,strings);results.add(metrics);
                System.out.println(json(metrics));
                var report=new LinkedHashMap<String,Object>();
                report.put("rows",rows);report.put("groups",groups);report.put("frames",frames);
                report.put("keyType",args[3]);report.put("workers",workers);report.put("shuffle","global Fisher-Yates of all input row ordinals before framing, seed 777");
                report.put("javaVersion",System.getProperty("java.version"));report.put("rounds",results);
                Files.writeString(output,json(report)+"\n");
            }
        } finally {executor.shutdownNow();}
    }
}

```

</details>

## Compact merge research and verification: 23 October 2026

The current implementation is `33e9c4b`, following the research below. The comparison uses local `main` at `8c60a35` (the complete pre-redesign source tree), previous review `b9f5d6d`, and the current implementation. Remote `origin/main` has a different model-refactor history and is not the performance baseline. Earlier experiments below remain historical records tied to their stated revisions.

### Decisions from primary sources

| Research | Application to Cascada |
|---|---|
| [DuckDB constant vectors](https://duckdb.org/docs/preview/internals/vector) store repeated data once. | STRING aggregate state now keeps one shared winner and allocates exception pages only where a group differs. AVG type metadata needs no per-group string slot when uniform. This is a Cascada adaptation, not a claim that DuckDB uses this exact aggregate representation. |
| [DataFusion grouped aggregation](https://arrow.apache.org/blog/2023/08/05/datafusion_fast_grouping/) explains typed state and allocation costs. | Keep primitive paged state, start its first page at 16 groups, cap later pages at 1,024 groups, and avoid doubling the whole payload. No object is allocated per group. |
| [Swiss-table metadata](https://abseil.io/about/design/swisstables) and [F14's layout tradeoffs](https://engineering.fb.com/2019/04/25/developer-tools/f14/) distinguish probes from payload access. | Keep hash-tag filtering and test a denser tag index. An 80% growth threshold reduced index capacity in relevant cases without losing correctness. This remains scalar Java probing, not a SIMD Swiss-table implementation. |
| [Global Hash Tables Strike Back](https://arxiv.org/abs/2505.04153) studies purpose-built concurrent aggregation. | Considered as an alternative, not transplanted: concurrent updates or local reductions would change the existing floating-point update-order contract. Cascada already keeps one state owner per group rather than duplicating it across worker-local maps. |
| [DuckDB external aggregation](https://duckdb.org/2024/03/29/external-aggregation) and [memory management](https://duckdb.org/2024/07/09/memory-management) address larger-than-memory state. | Streaming and smaller state help, but disk spill and global query-memory admission remain separate architectural work. They are not implemented by this change. |
| [LMAX Disruptor's wait strategies](https://lmax-exchange.github.io/disruptor/user-guide/) trade wake-up latency against CPU use. | No busy-spin event loop was added. CPU workers park when idle; two reusable frame buffers overlap retrieval and aggregation and apply backpressure. Zero buffering would remove that overlap. |

The other compaction is specific to this table: resizing needs only the low 32 hash bits because hash tags remain in the slot index. When the last validity word has a spare upper half, those bits share that word. Wider schemas retain a separate hash word. Tests cross the 32-, 64-, and 96-flag boundaries and force table growth.

STRING MIN/MAX now retains winners and one shared baseline value, rather than a dictionary of every distinct input string. Its state scales with groups and sparse pages, rather than with distinct losing candidates. SQL NULL remains represented by the existing group-presence flag; equal strings and Spark code-point ordering retain their semantics.

### Configurable multicore execution

Set `CASCADA_MERGE_WORKERS` to the requested partition/worker budget. The application owns a shared, prestarted CPU pool with `min(requested workers, JVM available processors)` threads, separate from virtual retrieval tasks. On a host exposing 100 processors, setting 100 permits 100 CPU workers; this host exposes only 16 logical processors. The default setting remains unchanged.

Large routing, independent group owners, local sorts, co-ranked sorted merging, and output writers use the executor. Small-frame routing and the single-partition fast path can execute in the producer. The pool bounds worker threads, not all process CPU or total query memory. Global aggregates without dimensions and one dominant group retain one update owner. This is multicore merging within one JVM; it does not add a distributed shuffle across machines.

Correctness tests exercise 100 partitions on an eight-thread test executor, high-worker routing, separate retrieval/CPU execution, validity-word ownership, and owned-executor shutdown. They establish correctness and wiring, not 100-core throughput.

### Verification and measurement scope

The clean eight-module Java 21 reactor passed: **708 registered tests, 680 executed, 28 skipped, zero failures/errors**. The opt-in external integration tests remain skipped.

All performance comparisons below use the same Temurin Java 21.0.12.1 build, `-Xms5g -Xmx5g -XX:+UseParallelGC`, one query per JVM, two warmups, and three measured rounds. Every query merges 20 million rows across 300 frames with exactly 12 input columns: three grouping columns and three distinct weighted AVG ingredient triplets. A global Fisher-Yates shuffle of all row ordinals (seed 777) occurs before framing. Counts vary, NULL sums and all-zero-count groups are included, and every output key and all three averages are checked against an independent oracle.

Generation, validation, and explicit GC are outside the merge timer. This is the production incremental `FrameMergeService` path, including finishing and output projection; it excludes cache retrieval/decoding, Spark, transport, and API serialization. The main branch is single-worker; its one-worker result is shown separately from the updated one-worker result so parallelism is not confused with layout improvements.

Retained heap is measured after full GC with both the merger and final output explicitly held live, subtracting the pre-merge heap while inputs are held. Allocated bytes count the caller plus prestarted worker threads. These are not process RSS or end-to-end memory. Sampled heap can include unused TLAB reservations and uncollected garbage, especially with many threads, so it is retained as diagnostic evidence rather than called live state size. All values below are medians of three measurements, not latency percentiles or universal guarantees.

### Measured speed, CPU work, and live state

Host: Ryzen 7 5700G, eight physical cores / sixteen logical processors. Main uses its existing one-worker implementation; current uses ten workers in this comparison.

| Input keys / groups | Main wall seconds | Current wall seconds | Wall speedup | Main retained MiB | Current retained MiB |
|---|---:|---:|---:|---:|---:|
| LONG / 1 million | 16.932 | 0.932 | 18.2x | 154.14 | 132.89 |
| STRING / 1 million | 20.642 | 1.588 | 13.0x | 146.32 | 125.33 |
| LONG / 5 million | 22.867 | 1.866 | 12.3x | 1,062.67 | 692.67 |

For LONG / one million groups, current **one-worker** time is 7.239 seconds versus main's 16.932 seconds (2.34x). Current ten-worker CPU work is 9.344 CPU-seconds versus main's 16.859, about 45% lower; allocated bytes fall from 449.66 to 238.37 MiB. More cores are active simultaneously, even though total CPU work falls. Against the previous review's ten-worker result (1.132 seconds, 188.66 MiB retained), current is about 18% lower in elapsed time and 30% lower in retained heap.

| Current workers | Wall seconds | Speedup over current one worker | CPU seconds | Retained MiB |
|---:|---:|---:|---:|---:|
| 1 | 7.239 | 1.00x | 7.188 | 139.00 |
| 2 | 3.799 | 1.91x | 7.891 | 138.57 |
| 4 | 2.082 | 3.48x | 8.438 | 138.57 |
| 8 | 1.153 | 6.28x | 9.188 | 138.81 |
| 10 | 0.932 | 7.77x | 9.344 | 132.89 |

A separate STRING-winner query uses two million rows, 1,000 groups, 300 frames, and distinct candidate labels. Previous review retained about 120 MiB at ten workers; current retained about 0.074 MiB, because losing labels no longer accumulate in a dictionary. Current ten-worker time is 0.045 seconds versus previous review's 0.130. Main already used compact state here: about 0.055 MiB and 0.096 seconds at one worker. Current one-worker time is 0.113 seconds. This small-query tradeoff rules out claiming universally lower memory or faster one-worker execution.

### Reproduce the compact-state comparisons

The exact harnesses and raw JSON are retained locally under `docs/benchmarks/compact-state-2026-10-23/` (ignored benchmark evidence). The complete harness sources are embedded below so the published README remains self-contained. `FINAL_SUMMARY.json` records case medians and revisions locally; all primary cells have three verified measured rounds. Build separate checkouts of `8c60a35`, `b9f5d6d`, and `33e9c4b` with the same Java 21 toolchain and `mvn -B -Pvalidation -pl lib_cache -am -DskipTests compile`. Save both Java sources into an evidence directory, compile them against each revision's cache and identity classes, and run each cell sequentially in a fresh JVM. No simultaneous builds or benchmarks were running.

```powershell
$classes = "lib_cache/target/validation/target/classes;lib_identity/target/validation/target/classes"
javac -cp $classes -d evidence CompactMergeBenchmark.java StringWinnerBenchmark.java
# rows, groups, frames, key type, workers, warmups, measured rounds, JSON path:
java -Xms5g -Xmx5g -XX:+UseParallelGC -cp "evidence;$classes" com.cascada.cache.benchmark.CompactMergeBenchmark 20000000 1000000 300 LONG 10 2 3 result.json
# Repeat current with workers 1,2,4,8,10; main with 1; previous review with 10.
# Additional shapes: STRING/1000000 and LONG/5000000, same row/frame counts.
java -Xms5g -Xmx5g -XX:+UseParallelGC -cp "evidence;$classes" com.cascada.cache.benchmark.StringWinnerBenchmark 2000000 1000 10 winners.json
```

<details><summary>CompactMergeBenchmark.java</summary>

```java
package com.cascada.cache.benchmark;

import com.cascada.cache.application.service.FrameMergeService;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.merge.*;
import com.cascada.cache.domain.model.hashing.HashComponents;
import com.cascada.cache.domain.model.query.*;
import com.cascada.cache.domain.model.time.TimeRange;
import java.lang.management.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/** Local reproducible harness: real incremental merge and typed AVG; no Spark/network in timer. */
public final class CompactMergeBenchmark {
    static final String ALIAS = "avg_value";
    static final com.sun.management.ThreadMXBean THREAD = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
    static final com.sun.management.OperatingSystemMXBean OS = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();
    record Dataset(List<ResultFrame> frames, CanonicalQueryObject query, int id) {}
    record Answer(ResultFrame frame, Object owner, int id) {}
    static FrameMergeService service;
    static volatile Object retainedAnswer;
    static long[] allocationThreads;
    static long allocated() { long sum=0;for(long n:THREAD.getThreadAllocatedBytes(allocationThreads))sum+=Math.max(0,n);return sum; }
    static long used() { return MEMORY.getHeapMemoryUsage().getUsed(); }
    static void fullGc() throws Exception { System.gc();Thread.sleep(30);System.gc();Thread.sleep(30); }
    static String json(Object v) {
        if (v == null) return "null";
        if (v instanceof Number || v instanceof Boolean) return v.toString();
        if (v instanceof Map<?,?> m) return "{" + String.join(",", m.entrySet().stream().map(e -> json(e.getKey().toString()) + ":" + json(e.getValue())).toList()) + "}";
        if (v instanceof Collection<?> c) return "[" + String.join(",", c.stream().map(CompactMergeBenchmark::json).toList()) + "]";
        return "\"" + v.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }
    static long gcCount() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(x -> Math.max(0,x.getCollectionCount())).sum(); }
    static long gcTime() { return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(x -> Math.max(0,x.getCollectionTime())).sum(); }
    static double base(int group, int query) { return (group % 1000) * 0.125 + query * 3.5; }
    static boolean empty(int group, int occurrence) { return group % 9973 == 0 || group % 97 == 0 && occurrence % 3 == 0; }
    static Dataset generate(int rows, int groups, int buckets, boolean strings, int id) {
        String[] keys = strings ? new String[groups] : null;
        if(strings)for(int g=0;g<groups;g++)keys[g]="q"+id+"-g"+g;
        int[] permutation=new int[rows];for(int row=0;row<rows;row++)permutation[row]=row;
        Random random=new Random(777L);
        for(int row=rows-1;row>0;row--){int at=random.nextInt(row+1),temp=permutation[row];permutation[row]=permutation[at];permutation[at]=temp;}
        List<ResultFrame> frames=new ArrayList<>(buckets);int index=0;
        for(int bucket=0;bucket<buckets;bucket++) {
            int size=rows/buckets+(bucket<rows%buckets?1:0);
            var b=ResultFrame.builder().expectedRows(size).column("group_id",strings?ColumnType.STRING:ColumnType.LONG)
                    .column("region_id",ColumnType.LONG).column("device_id",ColumnType.LONG);
            for(int metric=0;metric<3;metric++) {
                String alias="avg_value_"+metric;
                b.column(AverageIngredients.sumColumn(alias),ColumnType.DOUBLE)
                 .column(AverageIngredients.countColumn(alias),ColumnType.LONG)
                 .column(AverageIngredients.typeColumn(alias),ColumnType.STRING);
            }
            for(int j=0;j<size;j++,index++) {
                int ordinal=permutation[index],g=ordinal%groups,occurrence=ordinal/groups;
                if(strings)b.appendString(keys[g]);else b.appendLong(g+(long)id*groups);
                b.appendLong(g%101).appendLong(g%17);
                for(int metric=0;metric<3;metric++) {
                    long count=empty(g+metric*11,occurrence+metric)?0:(occurrence+metric)%5+1;
                    if(count==0)b.appendNull();else b.appendDouble((base(g,id)+metric*11.25+occurrence*(metric+1))*count);
                    b.appendLong(count).appendString("double");
                }
            }
            var frame=b.build();if(frame.columnNames().size()!=12)throw new AssertionError("input must have exactly 12 columns");frames.add(frame);
        }
        if(index!=rows)throw new AssertionError("incorrect input row count");
        var measures=new LinkedHashMap<String,AggregateFunction>();var specs=new ArrayList<String>();var output=new ArrayList<String>(List.of("group_id","region_id","device_id"));
        for(int metric=0;metric<3;metric++){String alias="avg_value_"+metric;measures.put(alias,AggregateFunction.AVERAGE);specs.add("AVG(value_"+metric+") AS "+alias);output.add(alias);}
        var metadata=QueryMetadata.globalAggregate().withAggregateSpecs(specs).withMeasureAggregates(measures);
        var query=new CanonicalQueryObject(HashComponents.of(List.of("group_id","region_id","device_id"),specs,List.of()),
                new TimeRange(0,buckets*7200L-1),PostProcessing.none(),metadata,
                "SELECT group_id,region_id,device_id,AVG(value_0),AVG(value_1),AVG(value_2) FROM benchmark GROUP BY group_id,region_id,device_id",List.of("benchmark"),List.of(),List.of(),output);
        return new Dataset(List.copyOf(frames),query,id);
    }
    static Map<String,Object> verify(Answer a,int rows,int groups,boolean strings) {
        var frame=a.frame();var expectedColumns=List.of("group_id","region_id","device_id","avg_value_0","avg_value_1","avg_value_2");
        if(frame.rowCount()!=groups||!frame.columnNames().equals(expectedColumns))throw new AssertionError("wrong shape "+frame.columnNames());
        int key=frame.columnIndex("group_id"),region=frame.columnIndex("region_id"),device=frame.columnIndex("device_id"),nulls=0;
        int[] averages=new int[3];for(int m=0;m<3;m++){averages[m]=frame.columnIndex("avg_value_"+m);if(frame.columnType("avg_value_"+m)!=ColumnType.DOUBLE)throw new AssertionError("wrong AVG type");}
        BitSet seen=new BitSet(groups);double checksum=0,maxError=0;
        for(int r=0;r<groups;r++) {
            int g;
            if(strings){String v=frame.stringAt(r,key);if(!v.startsWith("q"+a.id()+"-g"))throw new AssertionError("mixed query");g=Integer.parseInt(v.substring(v.indexOf("-g")+2));}
            else g=Math.toIntExact(frame.longAt(r,key)-(long)a.id()*groups);
            if(g<0||g>=groups||seen.get(g))throw new AssertionError("duplicate/missing key");seen.set(g);
            if(frame.longAt(r,region)!=g%101||frame.longAt(r,device)!=g%17)throw new AssertionError("corrupt composite key");
            for(int metric=0;metric<3;metric++) {
                long count=0,weighted=0;int occurrences=(rows-1-g)/groups+1;
                for(int occurrence=0;occurrence<occurrences;occurrence++)if(!empty(g+metric*11,occurrence+metric)){long n=(occurrence+metric)%5+1;count+=n;weighted+=n*occurrence;}
                int avg=averages[metric];
                if(count==0){if(!frame.isNullAt(r,avg))throw new AssertionError("all-null AVG");nulls++;}
                else {
                    if(frame.isNullAt(r,avg))throw new AssertionError("missing AVG");
                    double expected=base(g,a.id())+metric*11.25+(metric+1)*(double)weighted/count,actual=frame.doubleAt(r,avg),error=Math.abs(expected-actual);
                    if(error>1e-10*Math.max(1,Math.abs(expected)))throw new AssertionError("wrong weighted AVG group="+g+" metric="+metric+" actual="+actual+" expected="+expected);
                    checksum+=actual;maxError=Math.max(maxError,error);
                }
            }
        }
        if(seen.cardinality()!=groups)throw new AssertionError("missing groups");
        return Map.of("checkedGroups",groups,"checkedAverages",(long)groups*3,"nullAverages",nulls,"maxAbsoluteError",maxError,"checksum",checksum);
    }

    static Map<String,Object> trial(Dataset dataset, int trial, int rows, int groups, boolean strings) throws Exception {
        retainedAnswer=null;fullGc();
        long retained=used();AtomicLong peak=new AtomicLong(retained);
        var sampling=new java.util.concurrent.atomic.AtomicBoolean(true);
        Thread sampler=Thread.ofPlatform().daemon().start(()->{
            while(sampling.get()){peak.accumulateAndGet(used(),Math::max);try{Thread.sleep(5);}catch(InterruptedException e){return;}}
        });
        long allocated0=allocated(),cpu0=OS.getProcessCpuTime(),begin=System.nanoTime();
        var merge=service.incremental(dataset.query());
        for(ResultFrame frame:dataset.frames())merge.add(frame);
        long accepted=System.nanoTime();ResultFrame result=merge.finish();long ended=System.nanoTime();
        long cpu=OS.getProcessCpuTime()-cpu0,allocation=allocated()-allocated0;
        peak.accumulateAndGet(used(),Math::max);sampling.set(false);sampler.join();
        Answer answer=new Answer(result,merge,dataset.id());
        Map<String,Object> correctness=verify(answer,rows,groups,strings);
        retainedAnswer=answer;fullGc();long live=used()-retained;
        var metrics=new LinkedHashMap<String,Object>();
        metrics.put("trial",trial);metrics.put("warmup",trial<0);
        metrics.put("seconds",(ended-begin)/1e9);metrics.put("acceptSeconds",(accepted-begin)/1e9);
        metrics.put("finishSeconds",(ended-accepted)/1e9);metrics.put("cpuSeconds",cpu/1e9);
        metrics.put("engineAllocatedBytes",allocation);metrics.put("sampledExtraHeapBytes",peak.get()-retained);
        metrics.put("liveMergeAndOutputHeapBytes",live);metrics.put("correctness",correctness);
        retainedAnswer=null;
        return metrics;
    }

    public static void main(String[] args) throws Exception {
        int rows=Integer.parseInt(args[0]),groups=Integer.parseInt(args[1]),frames=Integer.parseInt(args[2]);
        boolean strings=args[3].equals("STRING");int workers=Integer.parseInt(args[4]);
        int warmups=Integer.parseInt(args[5]),rounds=Integer.parseInt(args[6]);Path output=Path.of(args[7]);
        THREAD.setThreadAllocatedMemoryEnabled(true);
        var ids=new ArrayList<Long>();ids.add(Thread.currentThread().threadId());
        ThreadFactory factory=r->{Thread t=Thread.ofPlatform().name("merge-worker-"+ids.size()).unstarted(r);ids.add(t.threadId());return t;};
        var executor=new ThreadPoolExecutor(workers,workers,0,TimeUnit.SECONDS,new LinkedBlockingQueue<>(),factory);
        executor.prestartAllCoreThreads();allocationThreads=ids.stream().mapToLong(Long::longValue).toArray();
        try {
            try {service=FrameMergeService.class.getConstructor(int.class,String.class,int.class,Executor.class)
                    .newInstance(300,"ts",workers,executor);}
            catch(NoSuchMethodException olderMain){if(workers!=1)throw new IllegalArgumentException("main supports one worker");service=new FrameMergeService(300,"ts");}
            Dataset dataset=generate(rows,groups,frames,strings,0);
            var results=new ArrayList<Map<String,Object>>();
            for(int round=-warmups;round<rounds;round++) {
                Map<String,Object> metrics=trial(dataset,round,rows,groups,strings);results.add(metrics);
                System.out.println(json(metrics));
                var report=new LinkedHashMap<String,Object>();
                report.put("rows",rows);report.put("groups",groups);report.put("frames",frames);
                report.put("keyType",args[3]);report.put("workers",workers);report.put("shuffle","global Fisher-Yates of all input row ordinals before framing, seed 777");
                report.put("javaVersion",System.getProperty("java.version"));report.put("rounds",results);
                Files.writeString(output,json(report)+"\n");
            }
        } finally {executor.shutdownNow();}
    }
}

```

</details>

<details><summary>StringWinnerBenchmark.java</summary>

```java
package com.cascada.cache.benchmark;

import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** Measures retention of distinct losing STRING MIN candidates, with an independent exact oracle. */
public class StringWinnerBenchmark {
    static List<ResultFrame> inputs;
    static ExecutorService executor;
    static int rows, groups, workers;
    static java.lang.reflect.Constructor<?> constructor;
    static boolean parallel;

    static Map<String,Object> run(int trial) throws Exception {
        CompactMergeBenchmark.retainedAnswer=null;CompactMergeBenchmark.fullGc();
        long retained=CompactMergeBenchmark.used();AtomicLong peak=new AtomicLong(retained);
        AtomicBoolean sampling=new AtomicBoolean(true);
        Thread sampler=Thread.ofPlatform().daemon().start(()->{
            while(sampling.get()){peak.accumulateAndGet(CompactMergeBenchmark.used(),Math::max);try{Thread.sleep(5);}catch(InterruptedException e){return;}}
        });
        long alloc0=CompactMergeBenchmark.allocated(),cpu0=CompactMergeBenchmark.OS.getProcessCpuTime(),start=System.nanoTime();
        Map<String,AggregateFunction> functions=Map.of("MIN(label)",AggregateFunction.MINIMUM);
        Object[] args=parallel?new Object[]{inputs.get(0),List.of("key"),functions,"ts",300,workers,executor}
                :new Object[]{inputs.get(0),List.of("key"),functions,"ts",300};
        TypedFrameAccumulator merge=(TypedFrameAccumulator)constructor.newInstance(args);
        for(ResultFrame input:inputs)merge.accept(input);
        ResultFrame result=merge.finish();long end=System.nanoTime();
        long allocated=CompactMergeBenchmark.allocated()-alloc0,cpu=CompactMergeBenchmark.OS.getProcessCpuTime()-cpu0;
        peak.accumulateAndGet(CompactMergeBenchmark.used(),Math::max);sampling.set(false);sampler.join();
        if(result.rowCount()!=groups)throw new AssertionError("group count");
        for(int key=0;key<groups;key++){
            int last=key+(rows-1-key)/groups*groups;
            while(last>=0&&last%97==0)last-=groups;
            String expected=last<0?null:"v"+(1_000_000_000L+rows-last);
            if(result.longAt(key,0)!=key||!Objects.equals(result.valueAt(key,1),expected)
                    ||result.longAt(key,2)!=(rows-1-key)/groups+1L)throw new AssertionError("key "+key);
        }
        CompactMergeBenchmark.retainedAnswer=new Object[]{merge,result};CompactMergeBenchmark.fullGc();
        long live=CompactMergeBenchmark.used()-retained;CompactMergeBenchmark.retainedAnswer=null;
        return Map.of("trial",trial,"warmup",trial<0,"seconds",(end-start)/1e9,"cpuSeconds",cpu/1e9,
                "allocatedBytes",allocated,"liveMergeAndOutputHeapBytes",live,"sampledExtraHeapBytes",peak.get()-retained,"verifiedGroups",groups);
    }

    public static void main(String[] args)throws Exception{
        rows=Integer.parseInt(args[0]);groups=Integer.parseInt(args[1]);workers=Integer.parseInt(args[2]);Path output=Path.of(args[3]);
        int[] order=new int[rows];for(int r=0;r<rows;r++)order[r]=r;
        Random random=new Random(481);
        for(int r=rows-1;r>0;r--){int j=random.nextInt(r+1),v=order[r];order[r]=order[j];order[j]=v;}
        inputs=new ArrayList<>();int at=0;
        for(int bucket=0;bucket<300;bucket++){
            int size=rows/300+(bucket<rows%300?1:0);
            ResultFrameBuilder b=ResultFrame.builder().expectedRows(size).column("key",ColumnType.LONG)
                    .column("MIN(label)",ColumnType.STRING).column("SUM(q)",ColumnType.LONG);
            for(int row=0;row<size;row++,at++){
                int ordinal=order[at];b.appendLong(ordinal%groups)
                        .appendString(ordinal%97==0?null:"v"+(1_000_000_000L+rows-ordinal)).appendLong(1);
            }
            inputs.add(b.build());
        }
        order=null;
        CompactMergeBenchmark.THREAD.setThreadAllocatedMemoryEnabled(true);
        ArrayList<Long> ids=new ArrayList<>();ids.add(Thread.currentThread().threadId());
        ThreadFactory factory=r->{Thread t=Thread.ofPlatform().unstarted(r);ids.add(t.threadId());return t;};
        var pool=new ThreadPoolExecutor(workers,workers,0,TimeUnit.SECONDS,new LinkedBlockingQueue<>(),factory);
        pool.prestartAllCoreThreads();executor=pool;CompactMergeBenchmark.allocationThreads=ids.stream().mapToLong(Long::longValue).toArray();
        try{
            try{constructor=TypedFrameAccumulator.class.getConstructor(ResultFrame.class,List.class,Map.class,String.class,int.class,int.class,java.util.concurrent.Executor.class);parallel=true;}
            catch(NoSuchMethodException old){if(workers!=1)throw old;constructor=TypedFrameAccumulator.class.getConstructor(ResultFrame.class,List.class,Map.class,String.class,int.class);}
            List<Map<String,Object>> results=new ArrayList<>();
            for(int trial=-2;trial<3;trial++){
                Map<String,Object> value=run(trial);results.add(value);System.out.println(CompactMergeBenchmark.json(value));
                Files.writeString(output,CompactMergeBenchmark.json(Map.of("rows",rows,"groups",groups,"workers",workers,"rounds",results))+"\n");
            }
        }finally{pool.shutdownNow();}
    }
}

```

</details>

### Stress and remaining limits

The current implementation passed a further 50 measured weighted-AVG rounds: one billion input-row visits, 50 million groups, and 150 million checked AVG outputs. Separate fixed-heap G1 runs with 300 and 20 frames also passed all measured checks. The previous dictionary-ID representation was removed; this does not establish the root cause of the earlier out-of-range ID or native GC crashes. Their records remain below.

No disk spill, cross-machine execution, or global memory-admission guarantee is claimed. Skew, bandwidth, NUMA placement, and finite group cardinality can limit scaling. The 100-core throughput curve still requires a 100-core host. Small queries also have parallel scheduling overhead; increasing workers is a tuning choice, not a guarantee of lower latency for every shape.

## Single-query thread scaling: 23 October 2026

This follow-up measures one query at different worker counts. Baseline is `c1392bf`; the updated implementation is `74b8acf`. Both were compiled and run with Temurin Java 21.0.12.1, a fixed 5 GiB heap, and Parallel GC on this Ryzen 7 5700G host (8 physical cores, 16 logical processors). Each cell uses a fresh JVM, two warmups, and five measured rounds, with the before/after cells run sequentially at each worker count.

The controlled query has 20 million input rows, 12 columns, one million groups, and three AVG outputs. The main LONG grouping key is permuted; region/device derive from it. Per-alias counts are fixed and values depend on the group. Every returned grouping column and AVG value is checked against independent formulas after each timed round. Generation, explicit pre-round GC, and verification are outside timing. This measures the real accumulator and fused AVG finish, excluding cache retrieval, decoding, Spark, transport, and API serialization.

### Completed 20-frame matrix

All rounds in these cells passed verification. Values are medians. Speedup compares the updated implementation with its own one-worker result.

| Workers | Previous merger (s) | Updated merger (s) | Updated speedup | Updated process CPU (s) | Updated sampled extra heap (MiB) |
|---:|---:|---:|---:|---:|---:|
| 1 | 7.743 | 7.148 | 1.00x | 6.844 | 282.3 |
| 2 | 5.482 | 3.996 | 1.79x | 7.422 | 304.2 |
| 4 | 3.906 | 2.257 | 3.17x | 8.016 | 322.3 |
| 8 | 3.513 | 1.863 | 3.84x | 11.516 | 316.0 |
| 10 | 3.683 | 1.938 | 3.69x | 13.000 | 354.1 |

The updated query scales from 7.15 seconds at one worker to 1.94 seconds at ten. At ten workers it also takes about half the baseline wall time and substantially less CPU. Eight workers were slightly faster than ten on this eight-core host. There is no linear-scaling or 10x/30x guarantee.

Sampled extra heap subtracts the used-heap reading before merge and is sampled every 5 ms. It includes uncollected garbage and output; it is not a live-object, allocation-byte, or process-RSS measurement. Working heap increases at some higher worker counts. Parallel sorted finishing trades an additional group-index buffer for parallel execution; output writers share one allocation and avoid copying per-worker result frames.

### What changed

- Large frames use asynchronous parallel hash/count/scatter with stable per-partition row order. Partition work depends on routing completion and its previous frame.
- Every owned row updates all its measures while its contiguous group state is hot, replacing separate state scans for each measure.
- Sorted partitions combine through a balanced merge tree. Co-ranking splits even the final merge across workers, following the approach described by [Siebert and Traff](https://arxiv.org/abs/1303.4312).
- Large results use disjoint, 64-row-aligned writers in one fixed-size allocation. Every validity word has one owner, and publication checks completeness and seals writers.

The clean eight-module reactor passed with **693 registered tests, 665 executed, 28 skipped, zero failures/errors**. Tests include exact floating-point ordering across parallel routing chunks, ten-worker typed keys, null/decimal AVG output, validity-word ownership, immutable output, and large-frame acceptance on a one-thread executor.

### 300-frame stress failure and runtime limitations

A separate 300-frame case uses the same row count and schema, with the permutation continuing across frame boundaries. The primary updated ten-worker run failed in its last measured round with an out-of-range string dictionary ID (`67108864` for a one-entry dictionary). Its earlier completed rounds and later clean repeats do not establish that this failure is fixed. An instrumented repeat detected neither overlapping partition owners nor a stored-group hash mismatch. A separate read-only memory probe passed. A further uninstrumented 50-round run passed every check: one billion measured input-row visits and 150 million checked AVG outputs. The cause of the earlier failure remains unresolved; this path is experimental pending explanation.

Earlier G1 runs crashed in native GC threads during input generation on both Oracle Java 21.0.6 and Temurin Java 21.0.12.1. The previous implementation also returned an extra group in Oracle Java 21.0.6 ten-worker runs. These failures are retained in local evidence. Switching collectors for the controlled matrix does not prove the G1 or oracle failures resolved, and production GC configuration was not changed.

### Selecting workers

For the application, set `CASCADA_MERGE_WORKERS=10`. Standalone callers must select both partitions and executor capacity, for example `new FrameMergeService(300, "ts", 10, executor)` with a ten-thread executor. The two-argument service constructor and static `TypedFrameAggregator.aggregate` remain single-worker paths. Global aggregation without dimensions retains one owner to preserve update order; a dominant group cannot be accelerated across owners by raising this setting.

See the [updated code walkthrough](docs/UNCOMMITTED_COMMIT_PLAN.md#follow-up-scaling-one-query-across-threads) for ownership and memory details. The earlier four-worker routing comparison below remains a historical measurement, using a different runtime/collector setup.


## Reproducing the thread-scaling experiment

Use separate checkouts of `c1392bf` and `74b8acf`, build both with the same Java 21.0.12.1 toolchain (`mvn -B -Pvalidation -pl lib_cache -am -DskipTests compile`), and compile the harness below against each checkout. Run cells sequentially, with no simultaneous test/build work. The matrix alternated before and after at each worker count. Both used `-Xms5g -Xmx5g -XX:+UseParallelGC`; neither used the application environment variable to select workers. The harness selects its fixed thread pool and accumulator partition count explicitly.

```powershell
javac -cp lib_cache/target/validation/target/classes ScalingBenchmark.java
foreach ($workers in 1,2,4,8,10) {
    java -Xms5g -Xmx5g -XX:+UseParallelGC -cp ".;lib_cache/target/validation/target/classes" ScalingBenchmark $workers 1000000 20
}
# Separate 300-frame checks use the last argument 300.
```

```java
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.merge.*;
import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.*;
public class ScalingBenchmark {
 public static void main(String[] args) throws Exception {
  int workers=Integer.parseInt(args[0]), groups=args.length>1?Integer.parseInt(args[1]):100000, frames=args.length>2?Integer.parseInt(args[2]):20; long totalRows=(long)groups*20; long globalRow=0;
  List<ResultFrame> inputs=new ArrayList<>(); Map<String,AggregateFunction> functions=new HashMap<>();
  for(int f=0;f<frames;f++) { int frameRows=(int)(totalRows/frames+(f<totalRows%frames?1:0));
   var b=ResultFrame.builder().expectedRows(frameRows).column("k",ColumnType.LONG).column("region",ColumnType.LONG).column("device",ColumnType.LONG);
   for(int a=0;a<3;a++) {String alias="avg"+a; b.column(AverageIngredients.sumColumn(alias),ColumnType.DOUBLE).column(AverageIngredients.countColumn(alias),ColumnType.LONG).column(AverageIngredients.typeColumn(alias),ColumnType.STRING); functions.put(AverageIngredients.sumColumn(alias),AggregateFunction.SUM);functions.put(AverageIngredients.countColumn(alias),AggregateFunction.SUM);functions.put(ColumnNames.identity(AverageIngredients.typeColumn(alias)),AggregateFunction.MINIMUM);}
   for(int r=0;r<frameRows;r++,globalRow++) {int k=frames==20?(int)(((long)r*7919+f*17)%groups):(int)((globalRow*7919)%groups); b.appendLong(k).appendLong(k%17).appendLong(k%3); for(int a=0;a<3;a++) b.appendDouble((double)(k+a+1)*(a+1)).appendLong(a+1).appendString("double");}
   inputs.add(b.build());
  }
  try(var pool=Executors.newFixedThreadPool(workers)) {
   for(int round=-2;round<5;round++) {
    System.gc(); Thread.sleep(100);
    long retained=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
    var peak=new java.util.concurrent.atomic.AtomicLong(retained); var sampling=new java.util.concurrent.atomic.AtomicBoolean(true);
    Thread sampler=Thread.ofPlatform().start(()->{while(sampling.get()){peak.accumulateAndGet(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory(),Math::max);try{Thread.sleep(5);}catch(InterruptedException e){return;}}});
    long cpu=((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime(); long start=System.nanoTime();
    var merge=new TypedFrameAccumulator(inputs.get(0),List.of("k","region","device"),functions,"ts",300,workers,pool);
    inputs.forEach(merge::accept); long accepted=System.nanoTime(); var result=merge.finishWithAverages(List.of("avg0","avg1","avg2"));
    double secs=(System.nanoTime()-start)/1e9, cpus=(((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime()-cpu)/1e9;
    sampling.set(false); sampler.join();
    if(result.rowCount()!=groups) throw new AssertionError("groups expected="+groups+" actual="+result.rowCount());
    for(int r=0;r<groups;r++){if(result.columnReader(0).longValue(r)!=r || result.columnReader(1).longValue(r)!=r%17 || result.columnReader(2).longValue(r)!=r%3)throw new AssertionError("key"); for(int a=0;a<3;a++)if(result.columnReader(3+a).doubleValue(r)!=r+a+1)throw new AssertionError("avg "+r);}
    System.out.printf(Locale.ROOT,"workers=%d frames=%d round=%d seconds=%.6f cpu=%.6f accept=%.6f finish=%.6f retainedHeap=%d sampledPeakHeap=%d sampledExtraHeap=%d verified=%d%n",workers,frames,round,secs,cpus,(accepted-start)/1e9,secs-(accepted-start)/1e9,retained,peak.get(),peak.get()-retained,groups);
   }
  }
 }
}
```

<details>
<summary>Complete primary matrix rounds, including the failed cell</summary>

```text

matrix-after-1-f20.log
workers=1 frames=20 round=-2 seconds=6.782432 cpu=7.000000 accept=5.913221 finish=0.869210 retainedHeap=1705679528 sampledPeakHeap=2021469320 sampledExtraHeap=315789792 verified=1000000
workers=1 frames=20 round=-1 seconds=7.068131 cpu=6.875000 accept=6.278401 finish=0.789730 retainedHeap=1681235936 sampledPeakHeap=1986190752 sampledExtraHeap=304954816 verified=1000000
workers=1 frames=20 round=0 seconds=7.403614 cpu=7.062500 accept=6.629609 finish=0.774005 retainedHeap=1681236136 sampledPeakHeap=1977239592 sampledExtraHeap=296003456 verified=1000000
workers=1 frames=20 round=1 seconds=7.021806 cpu=6.671875 accept=6.254857 finish=0.766949 retainedHeap=1681236136 sampledPeakHeap=1977239592 sampledExtraHeap=296003456 verified=1000000
workers=1 frames=20 round=2 seconds=7.281733 cpu=6.890625 accept=6.335045 finish=0.946688 retainedHeap=1681236136 sampledPeakHeap=1990666624 sampledExtraHeap=309430488 verified=1000000
workers=1 frames=20 round=3 seconds=7.100556 cpu=6.734375 accept=6.331929 finish=0.768628 retainedHeap=1681236872 sampledPeakHeap=1977240328 sampledExtraHeap=296003456 verified=1000000
workers=1 frames=20 round=4 seconds=7.148245 cpu=6.843750 accept=6.369998 finish=0.778247 retainedHeap=1681236928 sampledPeakHeap=1990667416 sampledExtraHeap=309430488 verified=1000000

matrix-after-1-f300.log
workers=1 frames=300 round=-2 seconds=6.805836 cpu=7.156250 accept=5.683125 finish=1.122711 retainedHeap=1706324768 sampledPeakHeap=1993589472 sampledExtraHeap=287264704 verified=1000000
workers=1 frames=300 round=-1 seconds=7.265615 cpu=6.812500 accept=6.305005 finish=0.960610 retainedHeap=1681692136 sampledPeakHeap=1970578520 sampledExtraHeap=288886384 verified=1000000
workers=1 frames=300 round=0 seconds=7.333512 cpu=6.828125 accept=6.297302 finish=1.036210 retainedHeap=1681691992 sampledPeakHeap=1970578376 sampledExtraHeap=288886384 verified=1000000
workers=1 frames=300 round=1 seconds=7.079654 cpu=6.609375 accept=6.240542 finish=0.839111 retainedHeap=1681691992 sampledPeakHeap=1970578376 sampledExtraHeap=288886384 verified=1000000
workers=1 frames=300 round=2 seconds=7.137131 cpu=6.812500 accept=6.388783 finish=0.748348 retainedHeap=1681691992 sampledPeakHeap=1984005408 sampledExtraHeap=302313416 verified=1000000
workers=1 frames=300 round=3 seconds=7.395485 cpu=6.859375 accept=6.502486 finish=0.892999 retainedHeap=1681693024 sampledPeakHeap=1997433464 sampledExtraHeap=315740440 verified=1000000
workers=1 frames=300 round=4 seconds=8.493526 cpu=7.484375 accept=7.457393 finish=1.036133 retainedHeap=1681693120 sampledPeakHeap=1970579504 sampledExtraHeap=288886384 verified=1000000

matrix-after-10-f20.log
workers=10 frames=20 round=-2 seconds=2.514989 cpu=16.281250 accept=1.698548 finish=0.816441 retainedHeap=1705679528 sampledPeakHeap=2082884936 sampledExtraHeap=377205408 verified=1000000
workers=10 frames=20 round=-1 seconds=2.347542 cpu=14.734375 accept=1.776084 finish=0.571458 retainedHeap=1681308408 sampledPeakHeap=2056485656 sampledExtraHeap=375177248 verified=1000000
workers=10 frames=20 round=0 seconds=1.971601 cpu=13.546875 accept=1.525468 finish=0.446134 retainedHeap=1681308992 sampledPeakHeap=2066076952 sampledExtraHeap=384767960 verified=1000000
workers=10 frames=20 round=1 seconds=2.018091 cpu=13.000000 accept=1.598619 finish=0.419472 retainedHeap=1681310944 sampledPeakHeap=2052651880 sampledExtraHeap=371340936 verified=1000000
workers=10 frames=20 round=2 seconds=1.938479 cpu=14.187500 accept=1.589843 finish=0.348636 retainedHeap=1681310944 sampledPeakHeap=2052651808 sampledExtraHeap=371340864 verified=1000000
workers=10 frames=20 round=3 seconds=1.684384 cpu=12.046875 accept=1.307191 finish=0.377194 retainedHeap=1681310944 sampledPeakHeap=2052651880 sampledExtraHeap=371340936 verified=1000000
workers=10 frames=20 round=4 seconds=1.639970 cpu=10.843750 accept=1.271077 finish=0.368893 retainedHeap=1694737976 sampledPeakHeap=2066078912 sampledExtraHeap=371340936 verified=1000000

matrix-after-10-f300.log
workers=10 frames=300 round=-2 seconds=2.264212 cpu=14.250000 accept=1.631472 finish=0.632741 retainedHeap=1705985568 sampledPeakHeap=2076617976 sampledExtraHeap=370632408 verified=1000000
workers=10 frames=300 round=-1 seconds=1.820433 cpu=11.593750 accept=1.425483 finish=0.394950 retainedHeap=1681744096 sampledPeakHeap=2027699720 sampledExtraHeap=345955624 verified=1000000
workers=10 frames=300 round=0 seconds=1.261702 cpu=10.562500 accept=1.023661 finish=0.238042 retainedHeap=1681743936 sampledPeakHeap=2049391712 sampledExtraHeap=367647776 verified=1000000
workers=10 frames=300 round=1 seconds=1.812554 cpu=11.781250 accept=1.487992 finish=0.324563 retainedHeap=1681745224 sampledPeakHeap=2025865816 sampledExtraHeap=344120592 verified=1000000
workers=10 frames=300 round=2 seconds=1.772679 cpu=10.765625 accept=1.464652 finish=0.308027 retainedHeap=1681745224 sampledPeakHeap=2037214864 sampledExtraHeap=355469640 verified=1000000
workers=10 frames=300 round=3 seconds=1.386243 cpu=10.093750 accept=1.170326 finish=0.215917 retainedHeap=1681746352 sampledPeakHeap=2041391168 sampledExtraHeap=359644816 verified=1000000
Exception in thread "main" java.lang.IndexOutOfBoundsException: Index 67108864 out of bounds for length 1
	at java.base/jdk.internal.util.Preconditions.outOfBounds(Preconditions.java:100)
	at java.base/jdk.internal.util.Preconditions.outOfBoundsCheckIndex(Preconditions.java:106)
	at java.base/jdk.internal.util.Preconditions.checkIndex(Preconditions.java:302)
	at java.base/java.util.Objects.checkIndex(Objects.java:385)
	at java.base/java.util.ArrayList.get(ArrayList.java:427)
	at com.cascada.cache.application.service.merge.TypedAggregationGroups.accumulateRow(TypedAggregationGroups.java:423)
	at com.cascada.cache.application.service.merge.TypedAggregationGroups.accumulateFrame(TypedAggregationGroups.java:233)
	at com.cascada.cache.application.service.merge.TypedFrameAccumulator.lambda$accept$3(TypedFrameAccumulator.java:138)
	at com.cascada.cache.application.execution.QueryExecutionScope.lambda$propagate$2(QueryExecutionScope.java:22)
	at com.cascada.cache.application.execution.QueryExecutionScope.lambda$propagate$3(QueryExecutionScope.java:23)
	at java.base/java.util.concurrent.CompletableFuture$UniRun.tryFire(CompletableFuture.java:787)
	at java.base/java.util.concurrent.CompletableFuture$Completion.run(CompletableFuture.java:482)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	at java.base/java.lang.Thread.run(Thread.java:1583)

matrix-after-2-f20.log
workers=2 frames=20 round=-2 seconds=4.351277 cpu=8.593750 accept=3.242929 finish=1.108348 retainedHeap=1705402144 sampledPeakHeap=2047794304 sampledExtraHeap=342392160 verified=1000000
workers=2 frames=20 round=-1 seconds=4.298508 cpu=7.765625 accept=3.149550 finish=1.148958 retainedHeap=1681298384 sampledPeakHeap=2007033304 sampledExtraHeap=325734920 verified=1000000
workers=2 frames=20 round=0 seconds=4.018842 cpu=7.328125 accept=3.108310 finish=0.910532 retainedHeap=1681298512 sampledPeakHeap=2000319928 sampledExtraHeap=319021416 verified=1000000
workers=2 frames=20 round=1 seconds=4.036586 cpu=7.546875 accept=3.121106 finish=0.915480 retainedHeap=1681298864 sampledPeakHeap=2000320280 sampledExtraHeap=319021416 verified=1000000
workers=2 frames=20 round=2 seconds=3.995918 cpu=7.234375 accept=3.096145 finish=0.899774 retainedHeap=1681298864 sampledPeakHeap=2000320280 sampledExtraHeap=319021416 verified=1000000
workers=2 frames=20 round=3 seconds=3.993403 cpu=7.421875 accept=3.090246 finish=0.903157 retainedHeap=1681298864 sampledPeakHeap=2000320280 sampledExtraHeap=319021416 verified=1000000
workers=2 frames=20 round=4 seconds=3.957726 cpu=7.421875 accept=3.031029 finish=0.926697 retainedHeap=1681298920 sampledPeakHeap=2013747368 sampledExtraHeap=332448448 verified=1000000

matrix-after-4-f20.log
workers=4 frames=20 round=-2 seconds=2.798992 cpu=11.031250 accept=1.925559 finish=0.873433 retainedHeap=1705679528 sampledPeakHeap=2054915736 sampledExtraHeap=349236208 verified=1000000
workers=4 frames=20 round=-1 seconds=2.462075 cpu=8.453125 accept=1.904672 finish=0.557403 retainedHeap=1681299520 sampledPeakHeap=2024614912 sampledExtraHeap=343315392 verified=1000000
workers=4 frames=20 round=0 seconds=2.256699 cpu=7.703125 accept=1.662405 finish=0.594294 retainedHeap=1681299768 sampledPeakHeap=2024615192 sampledExtraHeap=343315424 verified=1000000
workers=4 frames=20 round=1 seconds=2.414507 cpu=8.015625 accept=1.855512 finish=0.558994 retainedHeap=1681300296 sampledPeakHeap=2019244864 sampledExtraHeap=337944568 verified=1000000
workers=4 frames=20 round=2 seconds=2.329431 cpu=8.078125 accept=1.795095 finish=0.534336 retainedHeap=1681300296 sampledPeakHeap=2019244872 sampledExtraHeap=337944576 verified=1000000
workers=4 frames=20 round=3 seconds=2.227487 cpu=8.000000 accept=1.673028 finish=0.554460 retainedHeap=1681300848 sampledPeakHeap=2019245448 sampledExtraHeap=337944600 verified=1000000
workers=4 frames=20 round=4 seconds=2.185531 cpu=8.140625 accept=1.684021 finish=0.501510 retainedHeap=1681300904 sampledPeakHeap=2032672536 sampledExtraHeap=351371632 verified=1000000

matrix-after-8-f20.log
workers=8 frames=20 round=-2 seconds=2.637119 cpu=16.875000 accept=1.866170 finish=0.770950 retainedHeap=1705679528 sampledPeakHeap=2056312208 sampledExtraHeap=350632680 verified=1000000
workers=8 frames=20 round=-1 seconds=1.702809 cpu=10.812500 accept=1.231144 finish=0.471665 retainedHeap=1681305192 sampledPeakHeap=2017121936 sampledExtraHeap=335816744 verified=1000000
workers=8 frames=20 round=0 seconds=1.879299 cpu=11.515625 accept=1.453223 finish=0.426076 retainedHeap=1681305616 sampledPeakHeap=2012646688 sampledExtraHeap=331341072 verified=1000000
workers=8 frames=20 round=1 seconds=1.863146 cpu=11.828125 accept=1.467673 finish=0.395474 retainedHeap=1681306304 sampledPeakHeap=2026074440 sampledExtraHeap=344768136 verified=1000000
workers=8 frames=20 round=2 seconds=1.704080 cpu=10.718750 accept=1.300445 finish=0.403635 retainedHeap=1681307288 sampledPeakHeap=2012648408 sampledExtraHeap=331341120 verified=1000000
workers=8 frames=20 round=3 seconds=1.738217 cpu=10.656250 accept=1.349414 finish=0.388803 retainedHeap=1681307288 sampledPeakHeap=2012648392 sampledExtraHeap=331341104 verified=1000000
workers=8 frames=20 round=4 seconds=2.178209 cpu=13.312500 accept=1.678464 finish=0.499745 retainedHeap=1694734320 sampledPeakHeap=2026075424 sampledExtraHeap=331341104 verified=1000000

matrix-before-1-f20.log
workers=1 frames=20 round=-2 seconds=7.819927 cpu=8.171875 accept=6.922422 finish=0.897506 retainedHeap=1705402056 sampledPeakHeap=2006344344 sampledExtraHeap=300942288 verified=1000000
workers=1 frames=20 round=-1 seconds=7.252043 cpu=6.812500 accept=6.459017 finish=0.793026 retainedHeap=1681225784 sampledPeakHeap=1979789752 sampledExtraHeap=298563968 verified=1000000
workers=1 frames=20 round=0 seconds=7.899334 cpu=7.015625 accept=7.057474 finish=0.841860 retainedHeap=1694652552 sampledPeakHeap=1993216520 sampledExtraHeap=298563968 verified=1000000
workers=1 frames=20 round=1 seconds=8.167699 cpu=7.593750 accept=7.289828 finish=0.877871 retainedHeap=1681225592 sampledPeakHeap=1993216592 sampledExtraHeap=311991000 verified=1000000
workers=1 frames=20 round=2 seconds=7.743056 cpu=7.218750 accept=6.931747 finish=0.811310 retainedHeap=1681226328 sampledPeakHeap=1979790296 sampledExtraHeap=298563968 verified=1000000
workers=1 frames=20 round=3 seconds=7.074538 cpu=6.671875 accept=6.275982 finish=0.798555 retainedHeap=1681226328 sampledPeakHeap=1979790296 sampledExtraHeap=298563968 verified=1000000
workers=1 frames=20 round=4 seconds=7.065617 cpu=6.906250 accept=6.243189 finish=0.822428 retainedHeap=1681226384 sampledPeakHeap=1993217384 sampledExtraHeap=311991000 verified=1000000

matrix-before-1-f300.log
workers=1 frames=300 round=-2 seconds=7.599644 cpu=8.031250 accept=6.682525 finish=0.917119 retainedHeap=1705985488 sampledPeakHeap=2001776016 sampledExtraHeap=295790528 verified=1000000
workers=1 frames=300 round=-1 seconds=6.543650 cpu=6.343750 accept=5.765124 finish=0.778526 retainedHeap=1681681720 sampledPeakHeap=1969211336 sampledExtraHeap=287529616 verified=1000000
workers=1 frames=300 round=0 seconds=6.847605 cpu=6.390625 accept=5.974420 finish=0.873185 retainedHeap=1681681528 sampledPeakHeap=1969211144 sampledExtraHeap=287529616 verified=1000000
workers=1 frames=300 round=1 seconds=6.621242 cpu=6.781250 accept=5.812972 finish=0.808270 retainedHeap=1681681528 sampledPeakHeap=1969211144 sampledExtraHeap=287529616 verified=1000000
workers=1 frames=300 round=2 seconds=7.250732 cpu=6.734375 accept=6.298571 finish=0.952161 retainedHeap=1681681528 sampledPeakHeap=1982638176 sampledExtraHeap=300956648 verified=1000000
workers=1 frames=300 round=3 seconds=8.412363 cpu=7.593750 accept=7.400034 finish=1.012328 retainedHeap=1681682560 sampledPeakHeap=1982639224 sampledExtraHeap=300956664 verified=1000000
workers=1 frames=300 round=4 seconds=8.134313 cpu=7.015625 accept=7.162361 finish=0.971952 retainedHeap=1681682656 sampledPeakHeap=1969212272 sampledExtraHeap=287529616 verified=1000000

matrix-before-10-f20.log
workers=10 frames=20 round=-2 seconds=4.232188 cpu=25.203125 accept=3.031351 finish=1.200837 retainedHeap=1705679440 sampledPeakHeap=2061252912 sampledExtraHeap=355573472 verified=1000000
workers=10 frames=20 round=-1 seconds=3.849557 cpu=22.656250 accept=2.694000 finish=1.155557 retainedHeap=1681248592 sampledPeakHeap=2029492360 sampledExtraHeap=348243768 verified=1000000
workers=10 frames=20 round=0 seconds=3.682571 cpu=22.593750 accept=2.659691 finish=1.022880 retainedHeap=1681248936 sampledPeakHeap=2025017016 sampledExtraHeap=343768080 verified=1000000
workers=10 frames=20 round=1 seconds=3.962273 cpu=23.390625 accept=2.960801 finish=1.001471 retainedHeap=1681248936 sampledPeakHeap=2025017016 sampledExtraHeap=343768080 verified=1000000
workers=10 frames=20 round=2 seconds=3.649920 cpu=21.765625 accept=2.636900 finish=1.013020 retainedHeap=1681248936 sampledPeakHeap=2025017016 sampledExtraHeap=343768080 verified=1000000
workers=10 frames=20 round=3 seconds=3.788064 cpu=21.468750 accept=2.768589 finish=1.019475 retainedHeap=1681248936 sampledPeakHeap=2025017016 sampledExtraHeap=343768080 verified=1000000
workers=10 frames=20 round=4 seconds=3.617294 cpu=21.750000 accept=2.595755 finish=1.021538 retainedHeap=1694675968 sampledPeakHeap=2038444048 sampledExtraHeap=343768080 verified=1000000

matrix-before-10-f300.log
workers=10 frames=300 round=-2 seconds=2.671425 cpu=13.156250 accept=1.488028 finish=1.183398 retainedHeap=1705985488 sampledPeakHeap=2035922400 sampledExtraHeap=329936912 verified=1000000
workers=10 frames=300 round=-1 seconds=2.246417 cpu=9.703125 accept=1.327878 finish=0.918538 retainedHeap=1681712272 sampledPeakHeap=2015295000 sampledExtraHeap=333582728 verified=1000000
workers=10 frames=300 round=0 seconds=2.290927 cpu=11.046875 accept=1.404174 finish=0.886753 retainedHeap=1681712576 sampledPeakHeap=2013190832 sampledExtraHeap=331478256 verified=1000000
workers=10 frames=300 round=1 seconds=2.117670 cpu=10.718750 accept=1.337833 finish=0.779837 retainedHeap=1681712576 sampledPeakHeap=2005137816 sampledExtraHeap=323425240 verified=1000000
workers=10 frames=300 round=2 seconds=1.719673 cpu=10.421875 accept=1.002367 finish=0.717306 retainedHeap=1681712576 sampledPeakHeap=2005137816 sampledExtraHeap=323425240 verified=1000000
workers=10 frames=300 round=3 seconds=1.660899 cpu=9.609375 accept=0.973976 finish=0.686923 retainedHeap=1681712872 sampledPeakHeap=2031992168 sampledExtraHeap=350279296 verified=1000000
workers=10 frames=300 round=4 seconds=1.618544 cpu=9.796875 accept=0.935601 finish=0.682944 retainedHeap=1681712968 sampledPeakHeap=2005138208 sampledExtraHeap=323425240 verified=1000000

matrix-before-2-f20.log
workers=2 frames=20 round=-2 seconds=5.819520 cpu=11.015625 accept=4.406808 finish=1.412712 retainedHeap=1705679440 sampledPeakHeap=2035391120 sampledExtraHeap=329711680 verified=1000000
workers=2 frames=20 round=-1 seconds=5.248326 cpu=9.406250 accept=3.793791 finish=1.454535 retainedHeap=1681241328 sampledPeakHeap=2005861096 sampledExtraHeap=324619768 verified=1000000
workers=2 frames=20 round=0 seconds=5.544395 cpu=9.796875 accept=3.976389 finish=1.568005 retainedHeap=1681241456 sampledPeakHeap=1999147696 sampledExtraHeap=317906240 verified=1000000
workers=2 frames=20 round=1 seconds=5.556709 cpu=9.859375 accept=4.136428 finish=1.420281 retainedHeap=1681241456 sampledPeakHeap=1999147696 sampledExtraHeap=317906240 verified=1000000
workers=2 frames=20 round=2 seconds=5.482101 cpu=9.578125 accept=4.168759 finish=1.313342 retainedHeap=1681241672 sampledPeakHeap=1999147912 sampledExtraHeap=317906240 verified=1000000
workers=2 frames=20 round=3 seconds=5.296428 cpu=8.984375 accept=3.870256 finish=1.426172 retainedHeap=1681241672 sampledPeakHeap=2008099272 sampledExtraHeap=326857600 verified=1000000
workers=2 frames=20 round=4 seconds=5.342126 cpu=9.296875 accept=4.088332 finish=1.253794 retainedHeap=1681242464 sampledPeakHeap=2012575736 sampledExtraHeap=331333272 verified=1000000

matrix-before-4-f20.log
workers=4 frames=20 round=-2 seconds=4.242907 cpu=14.843750 accept=3.088874 finish=1.154033 retainedHeap=1705679440 sampledPeakHeap=2046710768 sampledExtraHeap=341031328 verified=1000000
workers=4 frames=20 round=-1 seconds=3.805390 cpu=11.921875 accept=2.765371 finish=1.040019 retainedHeap=1681248064 sampledPeakHeap=2008236184 sampledExtraHeap=326988120 verified=1000000
workers=4 frames=20 round=0 seconds=4.006199 cpu=12.515625 accept=2.811528 finish=1.194671 retainedHeap=1681248520 sampledPeakHeap=2001523112 sampledExtraHeap=320274592 verified=1000000
workers=4 frames=20 round=1 seconds=4.040404 cpu=12.734375 accept=2.961779 finish=1.078625 retainedHeap=1681248736 sampledPeakHeap=2001523328 sampledExtraHeap=320274592 verified=1000000
workers=4 frames=20 round=2 seconds=3.906394 cpu=12.093750 accept=2.808297 finish=1.098097 retainedHeap=1681248736 sampledPeakHeap=2001523328 sampledExtraHeap=320274592 verified=1000000
workers=4 frames=20 round=3 seconds=3.797331 cpu=12.265625 accept=2.799905 finish=0.997426 retainedHeap=1681248736 sampledPeakHeap=2001523328 sampledExtraHeap=320274592 verified=1000000
workers=4 frames=20 round=4 seconds=3.751988 cpu=12.140625 accept=2.796810 finish=0.955178 retainedHeap=1694675768 sampledPeakHeap=2014950360 sampledExtraHeap=320274592 verified=1000000

matrix-before-8-f20.log
workers=8 frames=20 round=-2 seconds=4.226091 cpu=22.203125 accept=2.898679 finish=1.327412 retainedHeap=1705679440 sampledPeakHeap=2025728832 sampledExtraHeap=320049392 verified=1000000
workers=8 frames=20 round=-1 seconds=3.614929 cpu=20.390625 accept=2.590537 finish=1.024392 retainedHeap=1681246816 sampledPeakHeap=2007393536 sampledExtraHeap=326146720 verified=1000000
workers=8 frames=20 round=0 seconds=3.508034 cpu=19.125000 accept=2.539410 finish=0.968624 retainedHeap=1681247488 sampledPeakHeap=2002918520 sampledExtraHeap=321671032 verified=1000000
workers=8 frames=20 round=1 seconds=3.882042 cpu=18.281250 accept=2.808454 finish=1.073588 retainedHeap=1681247488 sampledPeakHeap=2002918520 sampledExtraHeap=321671032 verified=1000000
workers=8 frames=20 round=2 seconds=3.513048 cpu=19.953125 accept=2.505331 finish=1.007717 retainedHeap=1681247488 sampledPeakHeap=2002918520 sampledExtraHeap=321671032 verified=1000000
workers=8 frames=20 round=3 seconds=3.475078 cpu=18.968750 accept=2.463438 finish=1.011639 retainedHeap=1681247488 sampledPeakHeap=2002918520 sampledExtraHeap=321671032 verified=1000000
workers=8 frames=20 round=4 seconds=3.547225 cpu=19.125000 accept=2.552525 finish=0.994700 retainedHeap=1681247544 sampledPeakHeap=2016345608 sampledExtraHeap=335098064 verified=1000000
```

</details>

The extended stress run uses the identical harness with the measured loop bound changed from 5 to 50. All 50 measured rounds passed; complete logs, failed-run diagnostics, the instrumented probe, compiler/runtime metadata, and SHA-256 checksums remain local under ignored `docs/benchmarks/thread-scaling-2026-10-23/`. The production code contains no diagnostic owner guard.

## Current code guide and merge review

Start with [the code walkthrough](docs/UNCOMMITTED_COMMIT_PLAN.md#start-here-how-the-current-code-works-23-october-2026). It follows a query through SQL metadata, safety checks, bucket retrieval, aggregation, AVG reconstruction, and output. It explains paged group state, partition ownership, cancellation, and memory limits.

The reviewed merge hashes each frame once and sends stable row ranges to their owning partitions. Two reusable routing buffers bound queued input retention. Tests cover typed keys, floating-point order, AVG, cancellation, backpressure, changing frame sizes, and page/hash-table growth.

The full earlier reviews and experiments below are historical records. Their findings apply to the snapshots they identify.

### Research and further scalability work

DuckDB's [parallel aggregation design](https://duckdb.org/2022/03/07/aggregate-hashtable) describes local tables and partitioned combination. Cascada uses partition ownership while preserving update order within each group. The workloads and representations differ, so DuckDB timings do not predict Cascada speed.

DuckDB's [external aggregation design](https://duckdb.org/2024/03/29/external-aggregation) is a reference for a future spill design. Cascada still needs memory accounting and spillable representations for object-backed keys and group state.

The current application separates virtual retrieval from a shared platform CPU worker pool. Small-frame producer work remains outside that pool; global memory admission and byte accounting across requests remain follow-up work. [Oracle's guide](https://docs.oracle.com/en/java/javase/21/core/virtual-threads.html) explains virtual threads' suitability for blocking work and limiting scarce resources separately.

[fastutil](https://github.com/vigna/fastutil) provides primitive collections worth evaluating for individual hot structures. Replacing the existing composite-key paged table needs a workload-specific comparison. This review adds no collection dependency.




## Merge routing verification: 23 October 2026

Summing Maven's module summaries, the clean Java 21 reactor build passed across all eight modules: **685 tests registered, 657 executed, 28 skipped, zero failures/errors**. The skips include opt-in external integration tests; this is not verification of a live Spark/Valkey/native deployment. The focused typed-merger suite has 13 passing cases, including cancellation, strict two-frame backpressure, 70,000-key page growth, and routing-buffer reuse with growing/shrinking/empty/skewed frames.

On this Windows host, forked Surefire returned a fork-launch error even after executing the identity tests. Verification therefore ran in the Maven JVM with its required Arrow/Agrona module opens, using the isolated validation profile:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
[xml]$taskPom = Get-Content pom.xml
$env:MAVEN_OPTS = $taskPom.project.properties.'agrona.jvm.args'
mvn -B -Pvalidation clean '-DforkCount=0' test
```

### Controlled routing measurement

This new comparison isolates the real `TypedFrameAccumulator` with fused AVG finishing. It is a different workload from the historical 300-bucket experiment below. Baseline is the original redesign (`c01ab014eaf6`, tree-identical to `d9a3bd2b254c`); reviewed code (`738768e`) uses stable routing and two reusable buffers. Both use a four-thread platform pool, Java 21.0.6, G1, `-Xms512m -Xmx5g` on the same host.

Each query merges **20 million rows, 12 columns, one million composite groups, 20 input frames**. Three LONG grouping columns are a permuted main key and two deterministic derivatives. Each group occurs once per frame. Three distinct SUM/COUNT/type triplets produce three AVG outputs, with fixed per-alias counts and group-dependent values. This tests high cardinality and routing; it does not model null-heavy inputs, changing weights, string/decimal keys, independent random dimensions, skew, or end-to-end retrieval.

Two warmups and five measured rounds run in each fresh JVM. Input generation, pre-round GC, and verification are outside the timer. Every main grouping key and all three AVG values are checked against independently computed formulas. That is 100 million measured rows and 15 million checked AVG values per version. The pool is warmed by the warmup rounds. The sampler runs every 5 ms; sampled heap includes garbage not yet collected, group state, sorting, and output. Extra heap subtracts the pre-merge used-heap reading. It is not allocated-byte accounting, a live-object measurement, process RSS, or a guaranteed maximum.

| Metric | Original redesign | Reviewed merger |
|---|---:|---:|
| Median merge time (s) | 3.307 | 3.150 |
| Median process CPU (s) | 12.641 | 12.094 |
| Median sampled extra heap (MiB) | 363.500 | 347.500 |
| Largest sampled extra heap (MiB) | 368.000 | 347.754 |

The median wall-time change is small and the round ranges overlap. This is a local comparison, not evidence of a general 10x/30x gain or a statistical latency guarantee. An intermediate implementation with fresh per-frame arrays increased sampled heap; it was replaced with bounded reusable buffers before publication. A four-buffer variant was faster in its exploratory run, but the final two-buffer variant favors lower retention. Those exploratory runs are not interchangeable with the final result.

Raw rounds (negative round numbers are warmups):

```text
BEFORE
workers=4 round=-2 seconds=3.603854 cpu=14.218750 retainedHeap=1764409928 sampledPeakHeap=2109538296 sampledExtraHeap=345128368 verified=1000000
workers=4 round=-1 seconds=3.414014 cpu=13.171875 retainedHeap=1763875144 sampledPeakHeap=2142161512 sampledExtraHeap=378286368 verified=1000000
workers=4 round=0 seconds=3.307214 cpu=13.015625 retainedHeap=1762838576 sampledPeakHeap=2137966656 sampledExtraHeap=375128080 verified=1000000
workers=4 round=1 seconds=3.589223 cpu=13.234375 retainedHeap=1762839184 sampledPeakHeap=2137967264 sampledExtraHeap=375128080 verified=1000000
workers=4 round=2 seconds=3.322758 cpu=12.640625 retainedHeap=1762839184 sampledPeakHeap=2143996592 sampledExtraHeap=381157408 verified=1000000
workers=4 round=3 seconds=3.119528 cpu=10.937500 retainedHeap=1762839184 sampledPeakHeap=2148455072 sampledExtraHeap=385615888 verified=1000000
workers=4 round=4 seconds=3.103831 cpu=11.093750 retainedHeap=1762839184 sampledPeakHeap=2148715152 sampledExtraHeap=385875968 verified=1000000
AFTER
workers=4 round=-2 seconds=3.633229 cpu=14.250000 retainedHeap=1764410024 sampledPeakHeap=2080555720 sampledExtraHeap=316145696 verified=1000000
workers=4 round=-1 seconds=3.289624 cpu=12.796875 retainedHeap=1763880488 sampledPeakHeap=2125129560 sampledExtraHeap=361249072 verified=1000000
workers=4 round=0 seconds=3.244631 cpu=12.437500 retainedHeap=1762843888 sampledPeakHeap=2125389056 sampledExtraHeap=362545168 verified=1000000
workers=4 round=1 seconds=3.150409 cpu=12.093750 retainedHeap=1762844496 sampledPeakHeap=2127224664 sampledExtraHeap=364380168 verified=1000000
workers=4 round=2 seconds=3.139173 cpu=11.750000 retainedHeap=1762844496 sampledPeakHeap=2127488864 sampledExtraHeap=364644368 verified=1000000
workers=4 round=3 seconds=3.205879 cpu=12.156250 retainedHeap=1762844824 sampledPeakHeap=2125129888 sampledExtraHeap=362285064 verified=1000000
workers=4 round=4 seconds=3.115895 cpu=11.937500 retainedHeap=1762844824 sampledPeakHeap=2127491256 sampledExtraHeap=364646432 verified=1000000
```

The harness below is preserved for reproduction. Save it as `RoutingBenchmark.java`, compile against each version's `lib_cache/target/validation/target/classes`, and run in a fresh JVM with that same class directory. Compile production sources with Java 21 first (`mvn -B -Pvalidation -pl lib_cache -am -DskipTests compile`). Compare separate checkouts of the baseline commit and reviewed branch, with no simultaneous builds or benchmarks. A Windows classpath example is:

```powershell
javac -cp lib_cache/target/validation/target/classes RoutingBenchmark.java
java -Xms512m -Xmx5g -cp '.;lib_cache/target/validation/target/classes' RoutingBenchmark 4 1000000
```

```java
import com.cascada.cache.domain.frame.*;
import com.cascada.cache.domain.merge.*;
import com.cascada.cache.application.service.merge.TypedFrameAccumulator;
import java.util.*;
import java.util.concurrent.*;
import java.lang.management.*;
public class RoutingBenchmark {
 public static void main(String[] args) throws Exception {
  int workers=Integer.parseInt(args[0]), groups=args.length>1?Integer.parseInt(args[1]):100000, frames=20;
  List<ResultFrame> inputs=new ArrayList<>(); Map<String,AggregateFunction> functions=new HashMap<>();
  for(int f=0;f<frames;f++) {
   var b=ResultFrame.builder().expectedRows(groups).column("k",ColumnType.LONG).column("region",ColumnType.LONG).column("device",ColumnType.LONG);
   for(int a=0;a<3;a++) {String alias="avg"+a; b.column(AverageIngredients.sumColumn(alias),ColumnType.DOUBLE).column(AverageIngredients.countColumn(alias),ColumnType.LONG).column(AverageIngredients.typeColumn(alias),ColumnType.STRING); functions.put(AverageIngredients.sumColumn(alias),AggregateFunction.SUM);functions.put(AverageIngredients.countColumn(alias),AggregateFunction.SUM);functions.put(ColumnNames.identity(AverageIngredients.typeColumn(alias)),AggregateFunction.MINIMUM);}
   for(int r=0;r<groups;r++) {int k=(int)(((long)r*7919+f*17)%groups); b.appendLong(k).appendLong(k%17).appendLong(k%3); for(int a=0;a<3;a++) b.appendDouble((double)(k+a+1)*(a+1)).appendLong(a+1).appendString("double");}
   inputs.add(b.build());
  }
  try(var pool=Executors.newFixedThreadPool(workers)) {
   for(int round=-2;round<5;round++) {
    System.gc(); Thread.sleep(100);
    long retained=Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
    var peak=new java.util.concurrent.atomic.AtomicLong(retained); var sampling=new java.util.concurrent.atomic.AtomicBoolean(true);
    Thread sampler=Thread.ofPlatform().start(()->{while(sampling.get()){peak.accumulateAndGet(Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory(),Math::max);try{Thread.sleep(5);}catch(InterruptedException e){return;}}});
    long cpu=((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime(); long start=System.nanoTime();
    var merge=new TypedFrameAccumulator(inputs.get(0),List.of("k","region","device"),functions,"ts",300,workers,pool);
    inputs.forEach(merge::accept); var result=merge.finishWithAverages(List.of("avg0","avg1","avg2"));
    double secs=(System.nanoTime()-start)/1e9, cpus=(((com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean()).getProcessCpuTime()-cpu)/1e9;
    sampling.set(false); sampler.join();
    if(result.rowCount()!=groups) throw new AssertionError("groups");
    for(int r=0;r<groups;r++){if(result.columnReader(0).longValue(r)!=r)throw new AssertionError("key"); for(int a=0;a<3;a++)if(result.columnReader(3+a).doubleValue(r)!=r+a+1)throw new AssertionError("avg "+r);}
    System.out.printf(Locale.ROOT,"workers=%d round=%d seconds=%.6f cpu=%.6f retainedHeap=%d sampledPeakHeap=%d sampledExtraHeap=%d verified=%d%n",workers,round,secs,cpus,retained,peak.get(),peak.get()-retained,groups);
   }
  }
 }
}
```

Local evidence and exploratory logs are retained under ignored `docs/benchmarks/merge-routing-2026-10-23/`, with `SHA256SUMS.txt`. The harness and final raw rounds are also included above so the published record is self-contained.

## Modules

| Module | Contents |
|---|---|
| `lib_identity` | Framework-free query identity: `QueryHash` |
| `lib_cache` | The cache domain + runnable engine: canonical query object, bucket math + pyramid, logic hashing, safety rules, columnar merge (`application/service/merge`), AVG reconstruction, cube subsumption + consistency verifier, warming queue + popularity tracker, coverage bitmaps, `ResultFrame` + Arrow/zstd serialization, in-memory + Lettuce/Valkey backends, `CacheExecutionEngine` |
| `lib_sql` | Calcite SQL compiler: canonical extraction (AVGâ†’SUM/COUNT, group-by/filter/time detection, HAVING/JOIN/DISTINCT logic signature, per-alias aggregate map), MySQLâ†’Spark dialect translation, gap-query builder, cache-correctness simulation gate |
| `lib_spark_config` | Pure three-knob (RAM/CPU/placement) Spark config derivation, golden-tested key-by-key |
| `lib_spark` | SparkSession assembly and the Spark/Delta execution adapter |
| `lib_fabric` | Cluster deployment: one self-contained YAML source applied via Fabric8 (ServiceAccount/RBAC, ConfigMaps, executor pod template, driver Deployment), tested against the Fabric8 mock server |
| `app` | Wiring. `mvn -Plocal-spark` bundles Spark 4.1.1 + Delta 4.1.0; Velox is the primary Linux execution backend (Java 21) |

## Model package convention

Production Java records live in a dedicated `model` package within their owning layer:
`domain/model/<subdomain>`, `application/model/<concern>`, or
`adapter/<direction>/<technology>/model`. Application settings live in `app/model/config`.
Services and ports reference these models; model placement does not change dependency direction.
A Java record represents data and can be a value object or DTO; being a record does not make it an entity.
Adapter models may use adapter technologies, while domain and application models remain framework-free.
The sealed `CompositeFormula` contract stays with its record implementations.
Test-local fixtures stay in tests. `AppArchitectureTest` checks record placement across the platform.

This is Cascada's package convention, based on the boundary separation described in
[Cockburn's original hexagonal architecture article](https://alistair.cockburn.us/hexagonal-architecture).

## Java and Agrona runtime

The project targets Java 21 and uses Agrona **2.6.1** (latest stable release verified on 4 October 2026).
Java does not need an upgrade for this Agrona release. Agrona 2.x requires these options on the JVM
running Cascada, including the Spark driver JVM:

```text
--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens=java.base/java.util.zip=ALL-UNNAMED
```

Keep the existing Arrow/Spark module options as well. Maven tests and the benchmark/resource runners
include the required options automatically. For an application launch, add them to `JAVA_TOOL_OPTIONS`
or the JVM startup arguments; for `spark-submit`, include them in `--driver-java-options` before the
Spark driver starts. Configuring driver JVM options after an in-process SparkSession starts is too late.

## Build and test

```bash
cd cascada
mvn -N install   # parent BOM, first time only
mvn test
```

For constrained local Spark comparisons against full-window SQL, run
`./scripts/Test-LocalSparkConsistency.ps1 -Rows 5000000` in PowerShell. The runner uses
two Spark task slots and a 1,536 MiB JVM heap. See the
[E2E SQL consistency report](#cache) for findings, coverage and current
logical SQL/cache/cube support boundaries.




## What the tests pin down

- AVG reconstruction divides summed SUM/COUNT, never averages averages.
- Bucket boundary algebra partitions head/body/tail with no gap or overlap; epoch resampling
  floors to step buckets, never to zero.
- The logic hash ignores the time range and clause order, but changes with aggregates, group-by,
  filters, step, HAVING, JOIN ON conditions, and DISTINCT. The canonical semantics version also
  separates identities produced by different projection-order rules.
- DISTINCT, HAVING, and JOIN bypass the cache entirely (`NON_MERGEABLE_SQL_FEATURE`): their
  per-bucket partials cannot be recombined into the whole-window answer.
- Warming stops at the last **complete** bucket boundary; a mid-bucket `warmEnd` never stores a
  partial bucket under a full-bucket key.
- A query with no aggregate bypasses the cache (`NO_AGGREGATION`) instead of having a GROUP BY
  fabricated for it.
- `MAX(latency) AS peak_latency` merges as MAX across buckets, and the cube consistency verifier
  rejects the old SUMmed answer.
- The zero-config derivation reproduces the golden `spark.json` key-by-key and never
  oversubscribes RAM.
- ArchUnit fails the build if a framework import enters a `domain` package.

## API process and existing library integration

`CascadaLauncher` starts persistent HTTP (8000) and gRPC (9090) APIs configured through environment variables and settings files.
The runtime wires the existing SQL translator/canonicalizer/gap rewriter, cache execution and
administration services, warming orchestrator, Spark executor, and Fabric cluster lifecycle ports.
Spark is initialized once and shared; each request and asynchronous cache gap runs on a separate
Java 21 virtual thread. Per-request Spark job groups isolate cancellation, including future jobs in
the cancelled group. A cancelled request does not stop the shared Spark session.

HTTP endpoints:

| Method | Endpoint | Existing operation |
|---|---|---|
| POST | `/query` | Logical SQL; `{"sql":"SELECT ...", "format":"json"}` or `"parquet"` |
| GET | `/cache/size` | Bytes, MB and bucket count |
| POST | `/cache/flush` | `{}` flushes Cascada buckets; `{"prefix":"QC:V4:..."}` selects a prefix |
| POST | `/cache/warm` | `{"start":0,"end":86399,"forceOverwrite":false}` runs the existing warmer |
| POST | `/cluster/deploy`, `/cluster/stop`, `/cluster/start`, `/cluster/restart`, `/cluster/delete` | Existing Fabric lifecycle operations; body is a map of deployment setting names to strings |
| GET | `/health` | API process health; does not start Spark or contact Kubernetes |

The gRPC contract is `app/src/main/proto/query.proto`. `QueryService.Query` streams typed protobuf
rows by default, or JSON/Parquet payload chunks. `CacheService` exposes measure/flush/warm;
`ClusterService.ChangeCluster` drives the existing lifecycle service. The first query message
contains column metadata, row count, content type, and cache-path status. Concatenate encoded payload
chunks to recover the JSON document or Parquet file. Protobuf null cells have no value set; decimals
use exact decimal strings. Invalid request shape returns HTTP 400; unsupported logical translation
returns 422. Server configuration/execution failures return safe 5xx errors.

There are **no application row/byte result ceilings or request concurrency gates**. Spark's
`spark.driver.maxResultSize` and the actual driver JVM/container resources are operator-controlled.
Fifty-five million rows are not rejected merely for their count. `ResultFrame` still materializes
primitive columns on the driver: results must fit the heap you provision. Response encoding streams
without constructing a second whole JSON document or row-map table. Network serialization remains
necessary. gRPC backpressure waits for the receiver or its cancellation/deadline.

## Grouped application configuration

Set `CASCADA_CONFIG_PATH` to the JSON application configuration. `server`, `warming` and `tables`
are bound once to immutable typed settings. `CASCADA_HTTP_PORT`/`CASCADA_GRPC_PORT` override file
ports; legacy engine environment settings continue to work.

```json
{
  "server": {"httpPort":8000,"grpcPort":9090,"shutdownTimeout":"PT30S"},
  "warming": {"enabled":true,"interval":"PT5M","lookback":"P7D","maximumPatterns":10000},
  "tables": [{
    "logicalName":"traffic", "deltaPath":"hdfs://lake/traffic",
    "columnMappings":{"ts":"ts","country":"C5","bytes":"C6"},
    "timeColumns":["ts"]
  }]
}
```

Configured physical time columns must match `CASCADA_TIME_COLUMN`, the existing cache merge clock.
`CASCADA_MERGE_WORKERS` sets how many hash partitions merge one query's cached frames in parallel
(default: half the hardware threads, between 1 and 4); results are identical for every value.
The [merge redesign benchmark report](#merge-redesign-measurements) records the implementation comparison and its measurement limits.
Use SQL aliases for the desired output column labels. Table configuration is loaded at startup;
this is not a new live table-registration feature. Warming runs only complete buckets and has
bounded, configurable observation retention. Disabling scheduled warming also disables automatic
query observation recording. Cache size/flush/warm remain available as existing admin operations.

`SPARK_CONFIG_PATH` accepts flat Spark JSON or grouped `common`/`delta`/`native` sections. It is
validated before binding API sockets, while actual Spark initialization remains lazy. Explicit
Spark environment overrides take priority, including `SPARK_DRIVER_MAX_RESULT_SIZE`.

## Single-file Fabric deployment

`lib_fabric/src/main/resources/fabric/deployment.yaml` is the only deployment resource source.
It embeds RBAC, Spark JSON, application JSON, Hadoop XML, logging, the executor pod template,
FAIR allocation XML, driver Deployment and Service. The default scheduler pool is explicitly FAIR
so concurrent jobs share the Spark task slots. One shared driver uses a headless service and
`Recreate` updates; multiple driver replicas behind that hostname are rejected.

Manifest substitution accepts simple scalar values and rejects quotes,
line breaks, and YAML/XML structural characters; percent-encode special characters in URLs.

`ClusterManifestRenderer.renderCombined(values)` produces one multi-document YAML stream;
Fabric8 applies the same documents. The source must be rendered before applying. The namespace,
image pull secret, storage and Valkey endpoint are supplied by the operator. Kubernetes access is
lazy; query-only startup does not open a control-plane client. No deployment was made during this work.

`CASCADA_DRIVER_MEMORY` controls the actual `-Xmx` and corresponding pod memory in the rendered
Deployment. `CASCADA_DRIVER_MAX_RESULT_SIZE` controls the Spark property (the supplied reference
value is 6g). Set these for your workload; the application does not substitute result-size limits.

## Java 21, Spark and Gluten/Velox

The reactor targets **Java 21**, **Spark 4.1.1 / Scala 2.13.17**, and the matching
**Delta 4.1.0 `delta-spark_4.1_2.13` artifact**. Shared Jackson, Arrow, Parquet, Hadoop, Protobuf and Netty versions
are aligned with that Spark runtime. A Java 17 image cannot run the release-21 application.
See [Spark 4.1.1](https://spark.apache.org/docs/4.1.1/) and
[Delta 4.1's Spark compatibility](https://github.com/delta-io/delta/releases/tag/v4.1.0).

Fabric configures `org.apache.gluten.GlutenPlugin`, native columnar shuffle, and an explicit
executor heap/off-heap/overhead split. `CASCADA_EXECUTOR_RAM_GB` and `CASCADA_GLUTEN_ENABLED`
control the existing memory derivation; the same compatible runtime image must be used by driver
and executors. AQE stays enabled. Driver and executor Java native/module-access options are explicit.

The Linux image must include a **Gluten Velox bundle built with `-Pdelta` for Spark 4.1 / Scala 2.13**
as `/opt/spark/jars/gluten.jar`, plus the matching Spark and Delta jars. The bundle is first
on the driver classpath so Gluten's Spark overrides take precedence. A Spark-3.5 bundle cannot be reused.
The generic Gluten 1.7.0 Spark-4.1 prebuilt bundle used in the historical benchmark lacks
the optional native Delta adapter. Build the coherent backend/package reactor with explicit
`-Pdelta`; adding only the ordinary Delta SQL extension does not supply that adapter.
See [the pinned build configuration](https://github.com/apache/gluten/blob/v1.7.0/pom.xml) and
[Delta support](https://github.com/apache/gluten/blob/v1.7.0/docs/get-started/VeloxDelta.md).
Java-17-built Gluten classes run on the Java 21 driver;
Cascada itself requires Java 21. Configuration validates the bundle's Spark minor version, Scala binary version
and Velox backend before session startup, and requires the Delta scan classes and backend component marker.
Native execution must run in Linux containers or WSL;
native Windows is rejected explicitly.
Keep the Spark image's executor entrypoint; the app Dockerfile retains it.

Velox is enabled by default by `SparkSessionConfigBuilder`. `CASCADA_GLUTEN_ENABLED=false`
selects JVM-only Spark for native Windows development. `CASCADA_GLUTEN_OFFHEAP_BYTES`
sets local native memory (default 536870912 bytes); `CASCADA_GLUTEN_JAR` adds a matching
bundle to `spark.jars` for executor distribution. The driver must also include that JAR
in its JVM classpath before it starts. Setting `spark.driver.extraClassPath` after an
in-process JVM has started cannot make plugin classes available.

The native profile defaults `spark.sql.ansi.enabled=false`: Gluten 1.7 otherwise falls back
for Spark 4's ANSI mode. This uses Spark's legacy invalid-cast/arithmetic behavior. Set the
property to `true` explicitly when ANSI errors are required, accepting native fallback.
Native sessions now default to `spark.gluten.memory.isolation=true` for concurrent-query
services and tracked automatic spilling. Gluten derives task-slot memory quotas from Spark's
resources. Operator overrides remain supported; executor classpaths also prepend the bundle.
Enabling Gluten without an explicit off-heap environment override preserves an existing
configuration-file budget. The [six-core local profile](config/spark-gluten-local.json) uses
6 GiB off-heap, ANSI=false, UTC and 48 shuffle partitions. Set `SPARK_MASTER=local[6]`
and `SPARK_CONFIG_PATH` to that file before starting the Linux JVM with a 3 GiB heap.

Build and validate locally through Docker:

```powershell
./scripts/Build-GlutenDeltaBundle.ps1 -Jobs 2
# Use the candidate directory printed by the successful build:
./scripts/Complete-GlutenDeltaValidation.ps1 -CandidateDirectory '<candidate-directory>' -RemoveBuildImage
```

The build records its image digest and profiles, builds native libraries first, then uses
Gluten's Maven wrapper. Validation checks actual native Delta scans before running real
Valkey warming/concurrency and the randomized 10 GB two-round benchmark. The
[central E2E README](#native-delta-e2e-record) contains the latest native Delta results,
dataset/schema, exact SQL, CPU/RAM settings and all measurement/warm-up times.

```sh
mvn -Pdistribution -pl app -am package -DskipTests
docker build --build-arg SPARK_RUNTIME_IMAGE=<matching-spark-delta-gluten-java21-image> -t cascada:0.1.0 .
```

## Verification

Unit/architecture and network tests cover the existing library graph, alias normalization for cube
reuse, error classification, warming, cache administration, cluster port calls, cancellation, and
52 simultaneous HTTP plus 52 simultaneous gRPC requests on virtual threads. Verified exact cube
hits return the original immutable frame; JSON reads primitive columns; gRPC chunks transfer owned
buffers without reusing published arrays. The columnar merge algorithm remains the existing engine.

Real local Spark tests exercise 52 independent job groups on one session, a million-row scalar codec
round trip, and an opt-in 55-million-row result/JSON encoding test. Enable native Spark tests with
`-Plocal-spark -Dcascada.spark.integration=true`; the large-result test additionally uses
`-Dcascada.large.result.integration=true`. Linux Velox offload, production HDFS/Valkey and live
Kubernetes remain environment-specific checks. Linux Docker validation subsequently passed
the native Delta gate, both 88-test cache/process suites and the full randomized 10 GB matrix.
See the [E2E README](#native-delta-e2e-record) for resource limits and unresolved stability findings.
## Local Spark, Velox and Valkey validation

Read [E2E_README.md](#native-delta-e2e-record) for the latest native Delta validation:
data types and distributions, row/byte counts, exact SQL, cache/cube boundaries,
Valkey warming/concurrency, every measured run and warm-up, native Delta configuration,
bundle location and reproduction commands. Raw and historical evidence is retained
in one verified local archive linked there. Runners write ignored temporary evidence and
refresh that single README; they do not create separate report Markdown files.

## Benchmark evidence

The reports below retain measured trials, commands, environment details and limitations. They are local merge or query measurements, not general latency or memory guarantees.

- [Merge redesign: before/after merger measurements](#merge-redesign-measurements)
- [20-million-row high-cardinality AVG report](docs/benchmarks/avg20m-2026-10-18/REPORT.md)
- [12-column high-cardinality AVG report](docs/benchmarks/avg20m-12cols-2026-10-18/REPORT.md) ([HTML](docs/benchmarks/avg20m-12cols-2026-10-18/REPORT.html))

## Consolidated design, E2E, and review records

The sections below retain the complete source documents. Heading levels and project-relative links were adjusted to fit this root README; the full original files and their SHA-256 values are preserved byte-for-byte under `.git/doc-consolidation-backup/manifest.json`.

- [Native Delta E2E results](#native-delta-e2e-record)
- [Architecture](#cascada-architecture-record)
- [Cache core fixes and parity review](#cache-core-fix-review)
- [Review remediation](#review-remediation)
- [Python parity review](#python-parity-review)
- [Freshness review](#freshness-review)
- [Readability and performance review](#readability-performance-review)
- [Spark result retrieval review](#spark-retrieval-review)
- [SQL and merge review](#sql-merge-review)

<!-- BEGIN CONSOLIDATED SOURCE: E2E_README.md -->
<a id="native-delta-e2e-record"></a>

Source document: `E2E_README.md` (original encoding: utf-8).

### Cascada E2E — latest native Delta results

Run date: 10 October 2026. Report simplified on 11 October 2026 (Europe/Paris).

Spark 4.1.1 · Delta 4.1.0 · Scala 2.13.17 · Java 21 · Gluten 1.7.0 / Velox built with `-Pdelta`.

[Data](#data) · [CPU and RAM](#resources) · [All timings in seconds](#timings) · [Exact SQL](#sql) · [Delta checks](#delta) · [Cache and Valkey](#cache) · [Gluten configuration and JAR](#configuration) · [Run it](#reproduce)

<a id="data"></a>
#### Data and all types

**144,215,577 event rows**, **10,009,780,673 compressed Parquet bytes** (10.009781 GB), excluding the Delta log. Spark wrote and reloaded actual Delta tables. Snappy compression; no padding field.
Seed: `2055636123002775237`. Ten append batches; each batch seed advances by 101. Each field uses a separate seeded random expression. Both engines read the same fixed dataset.
Epoch timestamps are LONG seconds in UTC: `[1759968000, 1762559999]`, spanning thirty days.

| Column | Spark type | Values |
| --- | --- | --- |
| event_id | long | Sequential unique event ID across batches. |
| ts | long | Epoch seconds: base + floor(random × 2592000). |
| region | string | 1.5% NULL; then 30% hot R000 branch; otherwise R000–R199. Overall R000 probability ≈29.895%. |
| device | string | D00–D63. |
| campaign | integer | 0–9999. |
| customer_id | long | 0–9999999. |
| amount | decimal(18,2) | 2% NULL; otherwise random [-5000,15000), rounded by decimal cast. |
| cost | decimal(18,4) | Random [0,10000), decimal cast. |
| units | long | Integer values 1–100. |
| latency | double | 2% NULL; otherwise [0,5000). |
| duration | double | [0,3600). |
| response_bytes | double | Floating measurement [0,1000000). |
| energy_kwh | double | [0,100). |
| temperature | double | [-20,60). |

All fourteen fields are nullable in the recorded Delta read schema. Only region, amount and latency deliberately generate NULLs. The separate 200-row Delta dimension has STRING region R000–R199 and STRING market north/south/central, determined by id modulo 3.

| Batch | Seed | Rows added | Total rows | Parquet bytes | Write seconds |
| --- | --- | --- | --- | --- | --- |
| 1 | 2055636123002775237 | 2000000 | 2000000 | 139764841 | 9.982101 |
| 2 | 2055636123002775338 | 16000000 | 18000000 | 1250189027 | 11.199923 |
| 3 | 2055636123002775439 | 16000000 | 34000000 | 2360603518 | 10.825413 |
| 4 | 2055636123002775540 | 16000000 | 50000000 | 3471024612 | 10.961659 |
| 5 | 2055636123002775641 | 16000000 | 66000000 | 4581439155 | 10.793985 |
| 6 | 2055636123002775742 | 16000000 | 82000000 | 5691854610 | 26.491993 |
| 7 | 2055636123002775843 | 16000000 | 98000000 | 6802263417 | 11.401852 |
| 8 | 2055636123002775944 | 16000000 | 114000000 | 7912680349 | 19.787269 |
| 9 | 2055636123002776045 | 16000000 | 130000000 | 9023088998 | 12.785069 |
| 10 | 2055636123002776146 | 14215577 | 144215577 | 10009780673 | 20.077630 |

Generation and count verification: 146.633450 seconds, excluded from query timings.

<a id="resources"></a>
#### CPU, RAM and measurement setup


| Setting | Value |
| --- | --- |
| Spark master / CPU | local[6] / 6 Docker CPUs |
| JVM heap | 3 GiB |
| Fixed off-heap | 6 GiB (6442450944 bytes) |
| Container memory cap | 12 GiB; includes heap, off-heap and runtime headroom |
| Shuffle partitions | 48 |
| Timezone / ANSI | UTC / disabled |
| AQE / scheduler | enabled / FAIR |
| Memory isolation / spill | enabled / auto |
| Samples | 2 per engine; fresh JVM-1 → Velox-1 → Velox-2 → JVM-2 processes |
| Timing unit | seconds; one warm-up followed by one measurement for each query/process |

Query times cover result collection, excluding JVM startup, data generation and hash/report work. Tables are not explicitly persisted; OS file caches may be warm. No Valkey cache is used in this 10 GB benchmark.

<a id="timings"></a>
#### Every latest measurement and warm-up, in seconds

##### Measured seconds


| Query | jvm-1 | velox-1 | velox-2 | jvm-2 |
| --- | --- | --- | --- | --- |
| wide-group-aggregates | 33.889618 | 12.260254 | 11.223971 | 35.127018 |
| hourly-group | 28.347838 | 10.614665 | 10.531121 | 30.857943 |
| filtered-cte-having | 9.090294 | 4.205638 | 4.331262 | 10.096634 |
| nested-cte-rollup | 27.047898 | 10.723443 | 10.731906 | 29.178227 |
| delta-dimension-join | 16.668494 | 4.677947 | 4.730345 | 17.009941 |
| cte-aggregate-join | 13.336916 | 3.237806 | 3.156086 | 14.205116 |
| in-subquery | 23.437245 | 8.502675 | 8.790384 | 23.755543 |
| high-cardinality-topk | 7.982495 | 4.223071 | 4.647473 | 7.486044 |

##### Warm-up seconds


| Query | jvm-1 | velox-1 | velox-2 | jvm-2 |
| --- | --- | --- | --- | --- |
| wide-group-aggregates | 42.425484 | 22.316936 | 18.299018 | 44.819763 |
| hourly-group | 28.575242 | 10.844867 | 10.982526 | 30.468656 |
| filtered-cte-having | 11.139695 | 5.150022 | 5.320618 | 11.653978 |
| nested-cte-rollup | 27.542363 | 10.757930 | 11.031297 | 30.345492 |
| delta-dimension-join | 17.564526 | 5.448067 | 5.428196 | 17.984680 |
| cte-aggregate-join | 13.739281 | 3.195696 | 3.342913 | 14.281226 |
| in-subquery | 24.515326 | 8.643776 | 8.525056 | 24.673390 |
| high-cardinality-topk | 9.325685 | 4.538047 | 4.929485 | 9.053319 |


| Query | JVM midpoint seconds | Velox midpoint seconds | Speedup |
| --- | --- | --- | --- |
| cte-aggregate-join | 13.771016 | 3.196946 | 4.308× |
| delta-dimension-join | 16.839217 | 4.704146 | 3.580× |
| filtered-cte-having | 9.593464 | 4.268450 | 2.248× |
| high-cardinality-topk | 7.734269 | 4.435272 | 1.744× |
| hourly-group | 29.602891 | 10.572893 | 2.800× |
| in-subquery | 23.596394 | 8.646529 | 2.729× |
| nested-cte-rollup | 28.113063 | 10.727675 | 2.621× |
| wide-group-aggregates | 34.508318 | 11.742112 | 2.939× |

**Combined: 163.758632 → 58.294024 seconds; 2.809× faster (64.40% less elapsed time).** Velox was faster on all eight queries. The midpoint is the arithmetic mean of the two samples. All 32 measurements and 32 warm-ups agreed; every native query used an actual Delta data scan.

<a id="sql"></a>
#### Exact executed SQL and output types

##### wide-group-aggregates

Result rows: 12,864. Native stages: 3. Native Delta scans: 1.

```sql
SELECT region,device,COUNT(*) AS n,COUNT(amount) AS present,SUM(amount) AS total,AVG(amount) AS mean,MIN(amount) AS lo,MAX(amount) AS hi,SUM(cost) AS costs,SUM(units) AS units,ROUND(AVG(latency),4) AS latency,ROUND(AVG(duration),4) AS duration,ROUND(AVG(response_bytes),4) AS bytes,ROUND(AVG(energy_kwh),4) AS energy,ROUND(AVG(temperature),4) AS temperature FROM events GROUP BY region,device ORDER BY region NULLS FIRST,device
```


| Output column | Spark type |
| --- | --- |
| region | string |
| device | string |
| n | long |
| present | long |
| total | decimal(28,2) |
| mean | decimal(22,6) |
| lo | decimal(18,2) |
| hi | decimal(18,2) |
| costs | decimal(28,4) |
| units | long |
| latency | double |
| duration | double |
| bytes | double |
| energy | double |
| temperature | double |

##### hourly-group

Result rows: 144,720. Native stages: 3. Native Delta scans: 1.

```sql
SELECT CAST(FLOOR(ts/3600)*3600 AS BIGINT) AS hour,region,COUNT(*) AS n,SUM(amount) AS total,AVG(amount) AS mean FROM events GROUP BY CAST(FLOOR(ts/3600)*3600 AS BIGINT),region ORDER BY hour,region NULLS FIRST
```


| Output column | Spark type |
| --- | --- |
| hour | long |
| region | string |
| n | long |
| total | decimal(28,2) |
| mean | decimal(22,6) |

##### filtered-cte-having

Result rows: 12,864. Native stages: 3. Native Delta scans: 1.

```sql
WITH filtered AS (SELECT region,device,amount,units FROM events WHERE ts>=1760400000 AND ts<1762128000 AND amount>0), grouped AS (SELECT region,device,SUM(amount) AS total,COUNT(*) AS n,SUM(units) AS units FROM filtered GROUP BY region,device HAVING COUNT(*)>100) SELECT * FROM grouped ORDER BY total DESC,region NULLS FIRST,device
```


| Output column | Spark type |
| --- | --- |
| region | string |
| device | string |
| total | decimal(28,2) |
| n | long |
| units | long |

##### nested-cte-rollup

Result rows: 201. Native stages: 5. Native Delta scans: 1.

```sql
WITH daily AS (SELECT CAST(FLOOR(ts/86400)*86400 AS BIGINT) AS day,region,device,SUM(amount) AS total,COUNT(*) AS n FROM events GROUP BY CAST(FLOOR(ts/86400)*86400 AS BIGINT),region,device), devices AS (SELECT region,device,SUM(total) AS total,SUM(n) AS n FROM daily GROUP BY region,device HAVING SUM(n)>20), regions AS (SELECT region,SUM(total) AS total,SUM(n) AS n,COUNT(*) AS devices FROM devices GROUP BY region) SELECT * FROM regions ORDER BY total DESC,region NULLS FIRST
```


| Output column | Spark type |
| --- | --- |
| region | string |
| total | decimal(38,2) |
| n | long |
| devices | long |

##### delta-dimension-join

Result rows: 192. Native stages: 4. Native Delta scans: 2.

```sql
SELECT d.market,e.device,SUM(e.amount) AS total,COUNT(*) AS n,AVG(e.amount) AS mean FROM events e JOIN regions d ON e.region=d.region GROUP BY d.market,e.device ORDER BY d.market,e.device
```


| Output column | Spark type |
| --- | --- |
| market | string |
| device | string |
| total | decimal(28,2) |
| n | long |
| mean | decimal(22,6) |

##### cte-aggregate-join

Result rows: 12,864. Native stages: 6. Native Delta scans: 1.

```sql
WITH devices AS (SELECT region,device,SUM(amount) AS total,COUNT(*) AS n FROM events GROUP BY region,device), totals AS (SELECT region,SUM(total) AS total,SUM(n) AS n FROM devices GROUP BY region) SELECT d.region,d.device,d.total,d.n,t.total AS region_total,t.n AS region_n FROM devices d JOIN totals t ON d.region <=> t.region ORDER BY d.region NULLS FIRST,d.device
```


| Output column | Spark type |
| --- | --- |
| region | string |
| device | string |
| total | decimal(28,2) |
| n | long |
| region_total | decimal(38,2) |
| region_n | long |

##### in-subquery

Result rows: 201. Native stages: 6. Native Delta scans: 2.

```sql
SELECT region,SUM(amount) AS total,COUNT(*) AS n FROM events WHERE device IN (SELECT device FROM events WHERE ts<1760054400 GROUP BY device HAVING AVG(units)>50) GROUP BY region ORDER BY region NULLS FIRST
```


| Output column | Spark type |
| --- | --- |
| region | string |
| total | decimal(28,2) |
| n | long |

##### high-cardinality-topk

Result rows: 100. Native stages: 2. Native Delta scans: 1.

```sql
SELECT campaign,CAST(FLOOR(customer_id/100000) AS BIGINT) AS customer_group,SUM(amount) AS total,COUNT(*) AS n FROM events WHERE ts<1760227200 GROUP BY campaign,CAST(FLOOR(customer_id/100000) AS BIGINT) HAVING COUNT(*)>=2 ORDER BY total DESC,campaign,customer_group LIMIT 100
```


| Output column | Spark type |
| --- | --- |
| campaign | integer |
| customer_group | long |
| total | decimal(28,2) |
| n | long |

Integer/decimal outputs and schemas were compared exactly. Floating AVG fields in the wide query are rounded to four decimal places by the displayed SQL. No percentile workload is included.

<a id="delta"></a>
#### Latest native Delta transaction checks

15 cases completed. The main fixture starts with 20000 rows: LONG id/ts/units, STRING g and DECIMAL(18,2) nullable randomized amount. Follow-up operations append ts=-1, add LONG score, update/delete/merge, overwrite with 100 rows and read historical versions. Mapping fixture: 2 rows, LONG renamed column and DECIMAL(18,2) amount. Deletion-vector fixture: 1000 LONG id/amount rows before deleting id%3=0. These are native Delta checks, separate from application cache reuse.

| Case | Native Delta scans |
| --- | --- |
| ordinary-aggregates | 1 |
| filtered-nested-cte | 1 |
| aggregate-cte-join | 2 |
| in-subquery | 2 |
| cube | 1 |
| empty-scalar | 1 |
| snapshot-current | 1 |
| time-travel-v0 | 1 |
| schema-evolution | 1 |
| delete-removed-files | 1 |
| merge-snapshot | 1 |
| overwrite-snapshot | 1 |
| time-travel-after-overwrite | 1 |
| column-mapping-rename | 1 |
| deletion-vectors | 1 |


<a id="cache"></a>
#### Latest cache, cube, sketches and real Valkey checks

One-million-row Spark-written Delta fixtures; latest JVM and native runs each completed 79 broad checks + 9 process tests = 88. The regular regression suite recorded 594 passes and 26 opt-in skips.

| Fixture | Columns and distribution |
| --- | --- |
| Broad cache/cube/sketch | LONG id/ts/amount/huge/user_id; STRING region/device; DOUBLE latency/absent; DECIMAL(20,3) price. Seven days of permuted epoch seconds, signed/null measures, integers above 2^53, 50021 repeated users. |
| Valkey/API | LONG ts/C_amount; STRING C_region/C_device; DOUBLE C_latency. One million rows over seven days; 5 regions plus NULL, 3 devices; signed amount NULL every 11th row; latency NULL every 13th row. |

Covered: complete-bucket warming, repeat/forced warming, head/body/tail merge, missing/corrupt bucket recovery, SUM/COUNT/MIN/MAX, weighted SUM/COUNT reconstruction, cube filter-down, ordering/top-k, HLL/KLL helpers, reconnect, measure/flush and foreign-key preservation.
32 concurrent hot HTTP/gRPC requests used zero Spark calls. Sixteen partial misses overlapped forced warming with separate Spark job groups; execution remained limited to six Spark slots. This is a one-million-row process test, not simultaneous full 10 GB queries. Valkey: one CPU, 192 MiB container, 128 MiB maxmemory, persistence off, no eviction.

| Mode | Hot p50 seconds | Hot p95 seconds | Partial+warm p50 seconds | Partial+warm p95 seconds |
| --- | --- | --- | --- | --- |
| JVM | 0.192773 | 0.223807 | 2.803460 | 3.397992 |
| VELOX | 0.250558 | 0.281661 | 1.790671 | 2.324430 |

Direct physical CTE/join SQL coverage does not imply those shapes are bucket-mergeable or supported by registered logical translation. Direct AVG is conservatively bypassed; ingredient reconstruction is separate. HLL/KLL remain standalone helpers. Delta mutations require cache invalidation/flush.

<a id="configuration"></a>
#### Gluten configuration and downloadable bundle


```json
{
  "spark.plugins": "org.apache.gluten.GlutenPlugin",
  "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension,org.apache.gluten.extension.GlutenSessionExtensions",
  "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
  "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
  "spark.executor.extraClassPath": "/opt/spark/jars/gluten.jar",
  "spark.memory.offHeap.enabled": "true",
  "spark.memory.offHeap.size": "6442450944",
  "spark.gluten.memory.isolation": "true",
  "spark.gluten.memory.dynamic.offHeap.sizing.enabled": "false",
  "spark.gluten.sql.columnar.backend.velox.spillStrategy": "auto",
  "spark.gluten.sql.columnar.maxBatchSize": "4096",
  "spark.sql.ansi.enabled": "false",
  "spark.sql.session.timeZone": "UTC",
  "spark.sql.shuffle.partitions": "48",
  "spark.default.parallelism": "6",
  "spark.sql.adaptive.enabled": "true",
  "spark.scheduler.mode": "FAIR"
}
```

Dynamic off-heap sizing stays disabled so the tested fixed 6 GiB budget applies. Put gluten.jar first on driver and executor classpaths before starting Java. Set SPARK_MASTER=local[6] and use -Xmx3g. The container cap is 12 GiB, not the JVM heap size.
Local JAR: [gluten.jar](C:/Users/omarm/.local/share/cascada/gluten-1.7.0-spark4.1-delta-7b8d3800/gluten.jar).
[Download the same JAR](https://github.com/omarMseddi0/cascada/releases/download/gluten-1.7.0-spark4.1-delta-linux-amd64/gluten.jar). Linux amd64 / local Delta build; native S3/GCS/HDFS/ABFS connectors are disabled.

<a id="reproduce"></a>
#### Run this configuration


```powershell
$bundle = Join-Path $env:USERPROFILE '.local/share/cascada/gluten-1.7.0-spark4.1-delta-7b8d3800/gluten.jar'
./scripts/Test-VeloxValkey.ps1 -Rows 1000000 -GlutenJarPath $bundle
./scripts/Test-DeltaTenGbBenchmark.ps1 -Rounds 2 -TargetBytes 10000000000 -GlutenJarPath $bundle
python ./scripts/Update-E2EReadme.py
```

Raw evidence: [local archive](C:/Users/omarm/.local/share/cascada/e2e-evidence-2026-10-10.zip). Earlier runs remain archived and are excluded from the displayed comparison. The published bundle remains a prerelease; this finite matrix does not establish universal SQL correctness or resolve every archived stability finding.


<!-- END CONSOLIDATED SOURCE: E2E_README.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/design.md -->
<a id="cascada-architecture-record"></a>

Source document: `docs/design.md` (original encoding: utf-8).

### Cascada architecture

Domain packages hold query, frame, identity and configuration values. Application services own
cache safety policies, query hashing, bucket planning, aggregation, cube reuse, warming and
configuration calculations. Infrastructure adapters implement the ports required by those services.
The runtime is plain Java 21 with HTTP and gRPC transports, Spark 4.1.1 and Delta 4.1.0.

```text
inbound adapter -> inbound port -> application service -> outbound port -> outbound adapter
                                      |
                                      v
                                  domain values
```

Compile-time dependencies point inward. `app/config/CascadaEngineFactory` wires the cache services
and their concrete adapters. `app/bootstrap/CascadaRuntime` composes the servers and cluster use cases
and owns process resources. Composition roots may import both port directions and concrete adapters;
inbound adapters and application services may not select infrastructure implementations.

#### Module ownership

| Module | Responsibility |
| --- | --- |
| `lib_identity` | Shared immutable identity values |
| `lib_cache` | Cache query and administration contracts, application algorithms, typed frame values and storage adapters |
| `lib_sql` | Catalog values and outbound Calcite/dialect implementations of cache SQL ports |
| `lib_spark` | Spark session configuration and the outbound query executor |
| `lib_spark_config` | Public configuration derivation contract, pure application services and configuration values |
| `lib_fabric` | Cluster settings and lifecycle contracts, application services, manifest and Kubernetes adapters |
| `app` | HTTP/gRPC/scheduling adapters, grouped settings, lazy infrastructure, composition and resource ownership |

SQL and Spark implement outbound ports owned by the cache application. They use its published
contracts and domain values, without depending on cache service implementations or cache adapters.
Fabric obtains executor memory calculations through its own `ExecutorMemorySizingPort`; the
composition root bridges that port to `DeriveSparkConfigurationUseCase`.

#### Package placement

`lib_cache/application/service` groups implementations under `safety`, `time`, `hashing`, `merge`,
`merge/columnar`, `cube` and `warming`. Supporting domain values retain their cohesive domain packages.
Ports contain interfaces only. Query and warming results live in `application/result`. Exceptions
live in each module's root `exception` package. Each authored production Java file declares one named
type; implementation helpers and builders have their own files. Generated protobuf code follows its
generator's structure and is excluded from the authored-type rule.

`BucketCachePort` declares hot storage operations. `CacheAdministrationPort` declares sizing and
flushing; backends implement the composite `CacheBackendPort`. Use cases receive the smaller contract
they need. A helper application algorithm does not require an additional interface unless it crosses
a real substitution boundary.

In the app, `config` holds immutable settings and engine composition. `adapter/out/configuration`
reads external configuration. Lazy query and cluster adapters receive factories from the composition
root. HTTP, gRPC and scheduling adapters call inbound ports. `ClusterValues` carries manifest values;
`ClusterSettingsService` performs the existing calculations and validation before rendering.
Legacy Fabric resource paths are retained; the active renderer loads `fabric/deployment.yaml`.

#### Performance constraints

Primitive column arrays, validity bitmaps, indexed aggregation loops, incremental merging,
copy-on-write frame snapshots, bounded UTF-8 caches and gRPC streaming remain in place.
Row-to-column encoding and decoding are shared without an intermediate row wrapper or extra maps.
Batch and incremental merging share final reconstruction and post-processing. The cube verifier
retains independent calculation of the expected answer so a planner defect cannot validate itself.

The factory shares application services, state, infrastructure and its virtual-thread task executor.
It also shares one gap rewriter between query execution and warming. Shutdown remains owned by the
composition/runtime boundary.

#### Verification

`pom.xml` sets `maven.compiler.release=21`; every module inherits it. Spark and Delta remain pinned
at 4.1.1 and 4.1.0. On this Windows host, run:

```powershell
./code-review/Invoke-ArchitectureMaven.ps1
python ./code-review/architecture-audit.py
```

The script invokes Maven directly through its Java launcher and performs `verify` using the
existing `distribution` profile and in-process tests with the required module opens. Isolated output
avoids external IDE builds overwriting Maven classes. The audit checks production package paths,
one named type per file, concrete types outside ports, core imports and Java 21 class-file headers.
ArchUnit enforces compiled dependency rules and excludes test output across build profiles.

Opt-in Spark integration tests remain opt-in. Ordinary tests exercise services, HTTP/gRPC behavior,
serialization, Spark mapping and the Fabric8 mock API; they do not establish behavior against a live
Valkey or Kubernetes installation. Package/import and formerly nested Java type names changed;
external Java callers need to migrate their imports. The HTTP/gRPC protocols are unchanged.

The dated review and benchmark evidence are in [architecture-review-2026-10-04.md](#benchmark-evidence).


<!-- END CONSOLIDATED SOURCE: docs/design.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/CACHE_CORE_FIX_REVIEW_2026-10-16.md -->
<a id="cache-core-fix-review"></a>

Source document: `docs/CACHE_CORE_FIX_REVIEW_2026-10-16.md` (original encoding: cp1252).

> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

### Cache core fixes and second parity review

Reviewed and repaired on 2026-10-16 (Europe/Paris). This report supersedes the open-finding status in `cache-python-parity-review-codex.md` and `freshness-review-codex.md`. Those documents describe the earlier snapshot; their reproduced bugs must not be read as current failures.

#### Scope and result

The Python `src/core/smart_cache` module (all 20 source/documentation files listed in the baseline review), `cashing_system_technical_details.md`, and `SMART_CACHE_README.md` were personally read by the primary agent. Java cache behavior was compared with that source and the two root documents. `review_claude.md` was excluded. The working tree already contained extensive unrelated changes; no reset, commit, deployment, or replacement of that work was performed.

Cascada retains the Python cache's core: time-independent query identity, complete bucket storage, exact head/tail queries, parallel cached/gap retrieval, typed aggregation, SUM/COUNT AVG ingredients, deferred ordering/limit, request-side read-only payload access, background-only bucket writes, and deduplicated recent/popular warming. The fixes below restore missing parity and repair freshness/schema errors. This is a cache-module review, not certification of every subsystem of Cascada.

#### Core comparison after repairs

| Contract | Final behavior | Relationship to Python |
|---|---|---|
| Identity and storage step | A configured fixed storage step; user display steps reconstruct from the same ingredients. Hash semantics version 4 isolates previous meanings. | Restores cross-granularity reuse. |
| Cold requests | Zero bucket hits execute the original physical query, including AVG. Requests do not write bucket payloads. | Preserved original fast path. |
| Partial/full hits | Complete cached buckets plus exact Spark gaps; missing/expired reads are recovered. Final public columns are projected after reconstruction. | Preserved core; stricter schema checks. |
| AVG | Store SUM/COUNT/type ingredients; compute final AVG after combining. Empty grouped results expose the requested alias and type. | SUM/COUNT is intentional; internal-column leakage is repaired. |
| Source policy | HOT_BATCH sources can cache; HOT_VIEW-only queries execute through Spark. Source matching uses registered physical paths. | Restored policy. |
| Warming votes | Eligible queries vote on hits and misses; the positive minimum-range exception can warm complete buckets. | Preserved; shared eligibility logic avoids a second policy implementation. |
| Historical validation | Compare the stored window, propose the next sliding window, acknowledge only after successful refresh. | Actual Python is sliding, despite the root README's frozen-epoch description. Failure handling is improved. |
| Warming failures | Failed patterns are requeued; baseline is not advanced; invalidation prevents old in-flight writes from restoring a flushed generation. | Repairs a shared failure class. |
| Cube accelerator | Enabled for supported global aggregates, with verified rollup, source/projection checks, fixed TTL, revision validation, and supporting-bucket expiry limits. | Java extension integrated into the bucket lifecycle. |

#### Concrete repairs

1. **Cube reuse and freshness.** The production factory enables the catalog. Fine-grained grouped answers can roll up to a requested subset with the correct output order. Reads validate the backend revision; registration rejects concurrent revisions. Fixed expiry does not slide on reads and cannot exceed supporting buckets' remaining TTL. Flush/backfill invalidation clears catalog and bucket state. AVG/composite/time-series shapes continue through the ordinary bucket path.
2. **Refresh integrity.** Canary acknowledgement follows successful warming, rather than preceding it. Forced refresh evicts superseded payloads and clears scoped Cascada bucket keys/coverage. In-memory and Valkey generation-checked writes reject computations started before invalidation. Cancellation propagates.
3. **Correct final schemas.** Empty AVG is reconstructed; output projection removes internal AVG/composite ingredients. Cold AVG executes native Spark SQL. Decimal AVG preserves its category. Arbitrary decimal composite arithmetic stays with Spark because precision/scale cannot be inferred from the cache's coarse column type.
4. **Fixed-step parity.** Hash normalization and warmer storage normalization align 300-second/hourly display variants. Storage rewriting touches only recognized projected/grouped time buckets; business predicates and time expressions inside measures remain intact.
5. **SQL boundaries and alias safety.** Arithmetic composites have private collision-resistant ingredient names. `lib_sql` compiles its existing Calcite tree into a SQL-independent typed arithmetic plan; `lib_cache` binds/evaluates that plan. The custom `CompositeExpression.java` text parser was removed. No second SQL grammar or reverse module dependency remains. Missing ingredients reject cache reconstruction and allow the authoritative fallback.
6. **Scoped SQL and safe wrapper reuse.** Registered table/column mappings respect joins, CTEs, derived scopes, wildcard-derived outputs, and explicit column alias lists. Proven distributive wrappers can flatten; other SQL executes through Spark. New features do not justify speculative bucket arithmetic.
7. **Restart migration.** Persisted popularity SQL is recanonicalized and rehashed using current semantics and the configured fixed step. Current-process registered keys are retained. Resolved current identities are remembered for retries and deduplicated. Production warming rechecks current source/safety policy, including cache-disabled configuration.
8. **Bounded overhead.** Coverage storage has family/byte limits and refuses pathological bitmap ordinals before allocation. Cube admission has a conservative 64 MiB budget. Popularity writes accumulate in a bounded local map and flush in batches. Typed incremental merging avoids retaining every decoded bucket frame. Expensive cube computation runs outside the catalog metadata lock.
9. **Request latency isolation.** Query observation uses a short canonical-registry lock; background Spark warming holds a separate cycle lock. A concurrency regression holds the warming executor blocked and proves a request can record its observation before that work is released.

#### SOLID and duplication

SQL parsing, dialect semantics, aggregate extraction, and formula compilation belong to `lib_sql`. Cache storage, typed combining, and evaluation belong to `lib_cache`; the shared formula records are domain contracts, independent of Calcite. Existing constructors remain compatible. Updating formula text invalidates a previously compiled plan instead of silently evaluating stale operations. Warming eligibility is shared with the read service through the safety registry. This change removes the duplicate parser without moving Spark/Calcite dependencies into the cache module or inventing an unnecessary generic expression framework.

#### Validation

The final reactor run finished at **2026-10-16 01:02:21 +02:00**, with **BUILD SUCCESS** for all seven modules. Maven reported **691 tests, zero failures, zero errors, 26 skipped** (665 executed). Property-test engine totals in the console include invocations that are not separately represented by XML test-case totals; the numbers here are the reactor console totals.

| Module | Reported tests | Skipped |
|---|---:|---:|
| Identity | 13 | 0 |
| Cache | 324 | 0 |
| Spark configuration | 23 | 0 |
| Fabric | 35 | 0 |
| SQL | 193 | 0 |
| Spark executor | 30 | 1 |
| Application | 73 | 25 |

The three real Valkey tests executed successfully. `CacheSqlSparkIntegrationTest` executed successfully (17.86 seconds). `WarmingFreshnessTest` passed all seven checks, including persisted-hash migration, current source policy, and observation during blocked warming; `WarmingRetentionTest` passed its concurrent snapshot checks. `git diff --check` passed (only an existing LF/CRLF conversion notice).

Skipped checks include the separate Spark serialization, 14-case local consistency suite, large-result, shared-session, and nine process-level Valkey/Spark tests. The active local Spark parity test is distinct from those skipped suites. Native Gluten/Delta and production-scale validation are not claimed.

The final synthetic merge run used ten 100,000-row input frames, 1,000 groups, three warmups and seven measured trials, with value/count correctness checks: **27.230 ms median**, **36,724,606 input rows/s**, **128 MiB heap limit**, **27.00 MiB sum of heap-pool peaks**, **0 ms measured GC during trials**. These are local microbenchmark measurements, not an end-to-end speed guarantee. The disposable Valkey test container was stopped after validation.

The reproducible reactor command is:

```powershell
mvn '-Pvalidation,local-spark' test '-DforkCount=0' `
  '-Dcascada.cache.spark.integration=true' `
  '-Dcascada.valkey.url=redis://127.0.0.1:16379'
```

Java 21 and Arrow/Spark module opens were supplied through `MAVEN_OPTS`; builds use isolated `target/validation/target` directories. Evidence: `codex-final-validation.log` and `codex-merge-benchmark.json`.

The Spark test compares direct Spark answers with cold, mixed, and fully warmed paths, checking column names/types and values. It covers decimal AVG, composites, display-step reuse, safe wrappers, empty AVG, CTEs/joins, mapped wildcard-derived outputs, and explicit column alias lists. The real Valkey tests cover independent adapters, revisions, TTL expiry, batched popularity, and stale warming-generation rejection. Unit/architecture tests cover failure acknowledgement, signatures, memory bounds, source policy, aliases, rollup, hash identity, and SQL/cache dependency direction.

#### Practical limits

These tests use local Spark 4.1.1 and a disposable Valkey instance. The Spark parity fixture substitutes temporary views for physical Delta paths; it does not validate native Gluten operators, multi-host execution, or real Delta transactional scans. The separate large-result/native/process suites remain opt-in, and skips must be reported. No production deployment was performed.

Freshness is bounded by configured bucket TTL and scheduled canary checks. It is not a claim of transactional snapshot isolation across arbitrary concurrent Delta mutations. The canary only detects changes visible to its configured validation query/window; aggregate canaries can miss offsetting changes. The sliding model is deliberate parity with actual Python.

The local merge benchmark measures typed in-process aggregation only, with retained synthetic inputs, 1,000 groups, and a 128 MiB JVM heap. Pool peaks are not process RSS; output cardinality and strings/decimals change memory cost. It proves neither a cluster throughput SLA nor an end-to-end speed multiplier. Spark still materializes a complete `ResultFrame`; an unbounded output contract cannot have constant driver RAM.

The separate `spark-retrieval-review-codex.md` explains the official Spark/Delta sources: direct `InternalRow` collection already avoids public external `Row` conversion; public Java has no PySpark-style Arrow collection switch. Iterator/Arrow/columnar alternatives require representative benchmarks. No unmeasured “10x” retrieval claim or collector replacement was introduced. Numeric reconstruction is exercised under the runtime's legacy arithmetic profile (`spark.sql.ansi.enabled=false`); ANSI overrides require separate parity qualification.


<!-- END CONSOLIDATED SOURCE: docs/CACHE_CORE_FIX_REVIEW_2026-10-16.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/REVIEW_REMEDIATION.md -->
<a id="review-remediation"></a>

Source document: `docs/REVIEW_REMEDIATION.md` (original encoding: utf-8).

### Review reconciliation and verified fixes

Reviewed on 13 October 2026 against the current working tree, starting from `ea85ec7`.
Sources: `review_claude.md` (11 October) and the sibling `CASCADA_REVIEW_AGAINST_PLAN.md`
(September review with subsequent audit additions).

The reports are evidence to investigate, not an implementation checklist. The September report
describes an older runtime; several of its claims no longer apply. Correctness defects, conditional
deployment risks, performance hypotheses, stylistic preferences, and missing roadmap features need
different treatment. This pass implements fixes to existing behavior only, without subagents.

#### Verified and corrected

| Finding | Actual correction |
|---|---|
| Negated/symmetric BETWEEN treated as an ordinary time bound | A shared `TimeBoundClassifier` accepts only non-negated asymmetric BETWEEN with integral bounds. Other forms remain business predicates, or bypass caching when no safe window exists. |
| Gap queries drop fractional predicates retained in the cache key | Canonicalization and rewriting use the same classifier. Regression tests check both sides of the contract. |
| Inner subquery windows can be overwritten | Derived-table time scopes are rejected for cache/gap rewriting rather than guessed. Existing logical translation already rejects nested sources. |
| Reserved time-column names produce invalid gap SQL | Gap bounds are constructed as AST nodes; the unparser consults parser keyword metadata when quoting identifiers. Ordinary names still use minimal quoting. |
| The one-argument factory ignores configured time dimensions | Both factory entry points now respect the configured map. |
| Cube OR predicates are mistaken for one equality literal | Equality accepts complete literals, and IN-list narrowing validates the list. Unsupported disjunctions fall through to ordinary execution rather than returning an empty cached answer. Quoted strings containing OR remain supported. |
| ORDER BY silently ignores a missing result column | Resolve supported case/quoting differences; reject unresolved or ambiguous columns so the engine can execute the authoritative SQL instead of applying LIMIT to arbitrary rows. |
| Prefix flush can delete foreign keys and tracker state | Require the Cascada bucket namespace and match only well-formed bucket keys. HTTP/gRPC reject invalid prefixes before invoking the flush port. |
| One cancellation callback aborts the rest | Isolate and log callback failures during cancellation. Check cancellation before authoritative fallback and subsequent Spark work. |
| Valid cache payload corruption can pass structural checks | Enable Zstandard checksums in both compression paths. A payload-corruption regression verifies checksum failure; old non-checksummed blobs remain readable. |
| Worker Errors can leave HTTP/gRPC clients hanging | Return a terminal error/status before rethrowing fatal worker errors. HTTP logs include the cause; client responses omit internal details. |
| Manifest settings can inject YAML/JSON/XML structure | Validate substitution keys and simple scalar values before rendering. Reject structural syntax instead of interpolating it. Unicode table names and paths remain supported. This does not restrict an operator's choice of image or host mount. |
| Previously written buckets can retain incorrect predicate results | Bump canonical semantics version from 2 to 3 so buckets written under the older rewriting rules are not reused. Existing entries remain stored until ordinary flush/retention handling. |

#### Findings that must not be applied blindly

- Large results are part of the engine's intended behavior. No returned-row cap, decoded-byte
  budget, request-concurrency gate, or new server-limit setting was added. A high row count alone
  does not prove a defect. Spark's existing result/resource settings remain operator-controlled.
- Typed column storage and aggregation already exist. The older claim that the active merger
  always builds row-map/list keys is stale for this working tree.
- Volatile/context-dependent query guards, exact cube numeric comparisons, schema checks,
  raw-time grouping, and nullable AVG fixes already exist. They were not rewritten as new work.
- Flush-all is already limited to Cascada bucket keys. The remaining defect was prefix flushing.
- Ordered projection signatures, alias preservation, and collision-free table replacement already
  address several September SQL findings. Java 17 and absent HTTP/gRPC/native-profile claims are
  also outdated for this working tree.
- Serial bucket jobs, cube locking, FIFO popularity eviction, bitmap allocation, and serializer
  buffering deserve workload-specific investigation. A source observation is not a measured
  performance regression, and a proposed replacement is not automatically faster or safer.
- Helper classes without a current caller are not automatically bugs. Do not delete public
  helpers or rewrite module boundaries simply to match a reviewer's preferred architecture.

#### Real issues still open, or requiring a separate product decision

- **Source freshness:** keys and cube answers do not bind to a defined Delta snapshot. Correctness
  after source mutations requires one consistent snapshot contract across the original query,
  gaps, warming, and cache identity. Neither a TTL nor an event-time "today" cutoff alone proves
  that contract. No watcher, lineage API, or guessed freshness policy was added.
- **Deployment security:** anonymous listeners, TLS, administrative authorization, RBAC, and
  host-mount policy remain concerns in their actual deployment context. Authentication, token
  configuration, and admission-control additions were removed to respect the instruction not to
  implement new features. The manifest fix addresses structural injection only; it does not make
  the service production-security complete.
- **Memory and infrastructure trust:** the driver still materializes frames, and cache decoding
  still depends on available memory and trusted infrastructure. Integrity checks do not solve
  compression bombs or replace a chosen resource policy. No arbitrary result-size restriction was
  introduced under the guise of fixing large-data support.
- **Operational behavior:** partial-hit refill/coverage repair, source-change invalidation, flush
  races, popularity eviction, and cancellation of already-running sibling work need their own
  reproductions and focused changes. They are not claimed solved by this pass.
- **Roadmap features:** tenant routing, UI/SDKs, approximate-query integration, tiered storage,
  cost policies, new schedulers, and general SQL/table-format support were not implemented.

#### Validation

JDK 21, direct Maven launcher, existing `validation,local-spark` profiles and required module opens:

```text
mvn -Pvalidation,local-spark -DforkCount=0 clean test
```

The clean full reactor passed: **645 cases reported by Maven, 619 passed, 26 skipped,
0 failures, 0 errors**. The skips are the existing opt-in native/Spark/Valkey/large-result tests.
After preserving Unicode manifest values, a focused Fabric reactor run also passed:
**35 Fabric tests and 23 Spark-configuration tests, 0 failures/errors**.
The final identifier-quoting follow-up passed **184 SQL tests**, along with Identity and Cache tests.
The final Cache check passed **290 Cache tests and 13 Identity tests** after preserving the existing
large-frame allocation behavior.

Logs: `target/review-full-tests.log`, `target/review-fabric-tests.log`, `target/review-sql-tests.log`,
and `target/review-cache-tests.log`.
No production deployment, external Valkey integration, or native benchmark was run.
The original review files and the existing commit history were preserved; these fixes are
uncommitted working-tree changes.


<!-- END CONSOLIDATED SOURCE: docs/REVIEW_REMEDIATION.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/cache-python-parity-review-codex.md -->
<a id="python-parity-review"></a>

Source document: `docs/cache-python-parity-review-codex.md` (original encoding: utf-8).

> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

> Historical review snapshot. Fix status and final validation are superseded by [the 2026-10-16 fix review](#cache-core-fix-review).

### Cascada cache: Python parity and correctness review

Reviewed personally by the primary agent on 2026-10-15 (Europe/Paris). This is a review, not a code repair. No production source was changed. The working tree contains extensive existing edits and was shared with another coding session. `review_claude.md` is excluded from all conclusions. Active subagents were stopped when the user requested that the primary agent personally read the Python module.

#### Verdict

The Java cache retains the main Python architecture: canonical query intent, time-independent identity, bucket keys, full-bucket reuse, exact partial edges, parallel cache/Spark gap fetches, SUM/COUNT AVG ingredients, typed combine operations, deferred ordering/limit, warmer-only bucket writes, deduplicated warming votes, and persistent popularity-based warming. It is not an unrelated implementation with only similar class names.

It is also not a complete behavioral port. Fixed-step cross-granularity reuse, HOT_BATCH-only eligibility and the range of SQL shapes cached have changed. The cube accelerator adds a separate answer cache without the bucket layer's expiry. Several freshness issues remain, but some were inherited from Python or contradict documentation in BOTH implementations. These distinctions matter: a passing test can accurately test a behavior that is different from the supplied README.

#### What was personally read

Both requested root documents were read fully: `data_collector/src/cashing_system_technical_details.md` and `data_collector/src/SMART_CACHE_README.md`.

Every file in `data_collector/src/core/smart_cache` was then read fully, including comments and helper code, in sequential chunks that avoided output truncation:

| File | Lines |
|---|---:|
| __init__.py | 22 |
| auto_warmer.py | 754 |
| cache_component_adapter.py | 120 |
| cache_execution_engine.py | 406 |
| cache_hashing.py | 145 |
| cache_manager.py | 207 |
| cache_safety_checker.py | 14 |
| cache_serialization.py | 64 |
| data_cache_adapter.py | 488 |
| domain.py | 155 |
| in_memory_simulation.py | 440 |
| interfaces.py | 145 |
| merging.py | 561 |
| query_tracker.py | 335 |
| safety_rules.py | 356 |
| sql_rewriter.py | 639 |
| time_utils.py | 124 |
| warming_queue.py | 92 |
| CACHE_INCONSISTENCY_EXPLAINED.md | 647 |
| QUERY_TRACKER_README.md | 190 |

These 20 files are the complete source-directory inventory returned by `rg --files`; no claim is made to have read every file in the much larger data_collector project. Related integration code outside this module remains a separate scope.

The primary agent also personally traced the corresponding current Java entry services, factory wiring, canonicalizer, classifier, translator, safety registry/rules, hashing, gap rewriting, execution engine, bucket calculator, warmer, data-change detector, merge/reconstruction, cube catalog, popularity adapter, Spark collector, mapper and Arrow serializer. Spark runtime performance was researched separately and its report was read and checked against the current collector and official Spark/Delta sources.

#### Confirmed remaining issues and parity departures

##### 1. P1: Cube answers bypass the bucket freshness lifecycle (new Java accelerator)

`lib_cache/.../application/service/CacheExecutionEngine.java:129-133` consults the cube before planning/checking bucket data. `application/service/cube/CubeShapeCatalog.java:84-103` stores whole-window results with no TTL, source generation or age. LRU is a capacity bound, not a freshness bound. Completed-window registration is a good existing fix, but historical Delta data can still change. The optional canary can clear the catalog on a detected change; that is not guaranteed when validation is absent, fails, or does not cover the changed history.

Direct reproduction in `docs/codex-review-probes/CubeFreshnessProbe.java`: authoritative value 100 is returned once and catalogued; change the authoritative value to 200; repeat the historical query with an empty bucket backend. It still returns 100 and total Spark calls remain 1. This accelerator can retain an answer after the Redis bucket TTL has expired, defeating the baseline's eventual refresh mechanism.

Recommendation: a common freshness generation/expiry across answer tiers, replacement of stale registrations, and explicit invalidation on relevant refresh. Merely verifying the arithmetic of an old cube cannot establish that its source data is current.

##### 2. P1: Failed forced refresh loses its refresh obligation (shared failure class)

Java's `warming/DataChangeDetector.java:80-91` persists its new baseline before `WarmingOrchestrator.java:159` invokes bucket warming. A failed pattern is requeued at lines 197-202, but that queue entry does not retain `forceOverwrite=true`. The next comparison sees the acknowledged signature unchanged, so the warmer skips an existing old bucket at lines 280-291. A transient failed overwrite can therefore leave known-stale data until TTL or another source change.

The Python implementation defers `_save_validation_baseline` until after its warming loops, which is better ordering, but `_warm_single_pattern_sequential` and `_write_single_bucket` catch failures and do not report unsuccessful refreshes to the caller. It can therefore acknowledge a new baseline after failed writes as well. This is an unresolved failure class in both, not proof that Java corrupted formerly reliable Python behavior. Java requeues failed patterns, an improvement, but the missing forced-refresh state means the retry can still skip stale keys.

Recommendation: track the dirty generation until affected buckets are successfully refreshed, or invalidate them before they can pass EXISTS-skip. The current finding is control-flow evidence, not a real Spark/Valkey failure-injection test.

##### 3. P2: Fixed-step identity does not produce cross-granularity reuse (confirmed Java parity gap)

Python `cache_hashing._normalize_projection_signature` replaces FLOOR bucket literals with `<STEP>`. The `/data` adapter removes the time bucket expression from the non-time group-by dimensions, while `generate_query_hash` supplies the configured fixed step. The Python warmer calls the engine without a user-step canonical argument, so its step rewriting targets fixed storage granularity. User gap fetches can use a coarser requested step as an optimization; these paths intentionally differ because only the warmer writes buckets.

Java sets the separate hash `step` field to 300, but `CalciteCanonicalObjectFactory.extractGroupBy` and `extractProjectionSignature` retain the full `FLOOR(ts/3600)*3600` expressions. `QueryHashGenerator` hashes those unnormalized fields. Java `GapQueryBuilder` does not rewrite the bucket step, and WarmingOrchestrator uses it directly. Consequently, the bucket payload and family remain tied to the original requested step.

`StepParityProbe.java` exercised current compiled classes:

```
step=300  hash=6f958c6ff8a85b80a0927d6778c84c16
step=3600 hash=3c33a10de879604a09ca0fc2f4288f0b
```

The generated daily SQL retained 300 and 3600 respectively. This avoids accidentally reading coarse data as fine data, so it is a reuse/cost departure rather than a demonstrated wrong-value bug. It contradicts the technical document's fixed-storage-step promise and current Python `/data` design.

Recommendation: normalize identity and write fixed-grain ingredients together, retaining the requested step only for final reconstruction. Do not change just the hash; that would let coarse payloads poison fine-grained requests.

##### 4. P2: HOT_BATCH-only eligibility is deliberately absent (explicit policy departure)

Python's `HotViewOnlyRule.check` bypasses when metadata `uses_hot_batch` is false, and SafetyChecker registers it. Both requested READMEs call this a critical policy. Java `QueryMetadata.java:14-19` explicitly removes this concept; `SafetyRuleRegistry.java:15-18,32-40` intentionally omits the rule. Registered tables do not carry equivalent source eligibility here.

If Cascada intentionally generalizes the product to arbitrary Delta sources, this can be a reasonable product change. It is still not the same eligibility contract. Restoring it requires a source classification/eligibility policy, not a check for a Python directory name. It is not evidence of incorrect SUM/AVG mathematics.

##### 5. P2: Empty grouped AVG leaks ingredient schema (reproduced; affects Java zero-hit path)

For `SELECT app, AVG(x) AS avg_x ... GROUP BY app` returning zero groups, Java rewrites AVG to private SUM/COUNT/type ingredients even on the direct AVG miss path. `IncrementalFrameMerge.java:45-49` returns an empty ingredient frame before reconstruction. `FrameMergeService.java:55-60` has an analogous all-empty early return.

`EmptyAverageProbe.java` output:

```
expected=[app, avg_x]
actual=[app, avg_x__cascada_avg_sum, avg_x__cascada_avg_count, avg_x__cascada_avg_type]
```

Python's `_reconstruct_avg_columns` also returns empty frames unchanged, so empty warmed ingredient schemas are not perfectly handled there either. However Python's zero-hit path runs the original physical SQL directly; Java's direct AVG miss path rewrites/merges ingredients, making that path exposed too. Preserve empty-result projection/type metadata rather than treating zero rows as a reason to skip public schema reconstruction.

##### 6. P2: SQL cache coverage is narrower than the Python module (safe bypass, not wrong-result proof)

Python `data_cache_adapter` walks wrapped SELECT/UNION trees and extracts composite aggregate formulas; `sql_rewriter` injects gaps into innermost data-reading SELECTs; `merging` reconstructs arithmetic composites.

Java's current `hasMergeableProjection` only admits direct aggregate calls or supported projected group expressions. The tested `SUM(a)+SUM(b) AS total` query receives `UNSUPPORTED_MERGE_SHAPE`, and NonMergeableSqlFeatureRule bypasses it. A tested derived-table query is rejected with `derived-table time scopes cannot be safely rewritten for caching`. The factory accepts simple SELECT roots; UNION/CTE roots do not enter ordinary bucket caching. Logical translation additionally rejects nested SELECT scopes and multi-table FROMs.

Conservative bypass is preferable to incorrect decomposition, and some complex SQL is not mathematically mergeable. But supported Python-style composite and wrapped endpoint patterns are not fully ported. Also, translation errors occur before ExecuteLogicalQueryService's canonicalization fallback block: a logical query that cannot be translated is not automatically a successful direct Spark fallback. Do not advertise blanket support based on dormant gap-builder branches or merge helper classes.

Recommendation: enumerate supported shapes explicitly and port only those for which whole-window/direct and cached execution can be proved equivalent. Add mixed-hit tests through the actual logical entry point, not only direct merger fixtures.

#### Important findings that are NOT Java regressions

- **Sliding validation:** Python `auto_warmer.py` explicitly performs the stored-window check, then re-baselines to the current window every successful cycle. Java does the same. Both differ from SMART_CACHE_README §4.4's frozen epoch/no-write-on-match requirements. `EpochProbe.java` proves two saves and three validation queries for an initial cycle plus an unchanged shifted cycle. Ordinary window movement does NOT itself trigger a false mismatch: both compare the stored window against its own prior signature first.
- **Warming on hits:** Python manager votes every non-bypassed query, and warming_queue documents hits as useful signals. Java's pre-execution vote is source parity, despite an older README path table saying full hits do not enqueue.
- **High-cardinality matching:** both implementations use exact membership for configured group-by column names. Java has not introduced that rule's case-sensitivity. Qualified names can still interact differently because the adapters represent identifiers differently. Treat normalization as hardening, not an established newly introduced regression.
- **Only bucket writes are warmer-owned:** the Python tracker writes popularity metadata in a background batch; Java queues asynchronous popularity commands per request. Neither implies that the request handler writes bucket payloads. Java's per-query network command volume is higher than the Python batched tracker and should be measured if popularity recording is significant.
- **Python documents are not an execution oracle:** CACHE_INCONSISTENCY_EXPLAINED.md says epoch-origin resampling, global deduplication and precision-independent datetime conversion were fixed. The current merging.py still uses `pd.Grouper` without explicit epoch origin in that branch, only deduplicates the preserve-raw branch, and retains `astype('int64') // 10**9`. Its text and code differ. The simulation intentionally bypasses some production guards and contains a 1900-second scenario incompatible with fixed step 300. A simulation result cannot substitute for the safety+engine entry path.

#### Preserved or improved mechanisms

| Contract | Current Java assessment |
|---|---|
| Exclude request time/LIMIT/ORDER from base logical identity | Present; source/projection and semantics version protect identity further |
| Full-body miss runs Spark rather than issuing one query per missing day | Present in current engine |
| Full buckets plus exact partial edges | Present; completed-bucket clock guard is stronger than Python's bare bucket math |
| Parallel cached payload and Spark gap work | Present; vanished EXISTS-to-read buckets trigger a supplemental gap query |
| User path does not store bucket data | Preserved; cube registration is a separate in-memory answer cache |
| Sequential complete-bucket warmer, TTL, empty results | Present; Java adds row cap and rejects incomplete trailing buckets |
| AVG as SUM plus COUNT of non-null expression | Present, with Java decimal/type handling; empty-schema exception noted above |
| SUM/COUNT/MIN/MAX combine by parsed operation | Present; Java avoids relying only on alias prefixes |
| Merge before ordering and limiting | Present for admitted shapes; unresolved ordering throws rather than silently dropping it |
| Persistent top-N and per-cycle deduplication | Present; Java persists physical SQL and reconstructs canonical queries after restart |
| Binary compressed payload | Arrow IPC + Zstd replaces pickle + Zstd; preserves the design objective, not Python blob interoperability |

#### Verification and limits

The targeted current Java tests reported 32 passed, zero failures/errors and Maven BUILD SUCCESS: DataChangeDetectorTest, WarmingFreshnessTest, FramePostProcessingTest and TypedMergeRegressionTest. Java 21 with in-process Surefire and the necessary module opens was used after fork setup failures. The tool wrapper still reported exit 1 for a Maven-success invocation, so the report records both facts.

The primary agent compiled and ran the standalone CubeFreshnessProbe, EpochProbe, EmptyAverageProbe and StepParityProbe against existing compiled classes; their sources are retained under `docs/codex-review-probes`. The first three use fake ports or direct merger input; the step probe uses the current Calcite factory and gap builder. These are focused reproductions, not a real Spark cluster benchmark.

The Python module was fully read but was not executed: the bundled Python environment lacks sqlglot/Redis/Zstandard dependencies, and no project environment was found at the usual local venv paths. No claim of a passing Python differential suite is made. Full production Spark/Delta/Gluten/Valkey validation was not run. The two foundational specifications contain conflicting historical examples, so the report distinguishes actual source parity, normative document differences and provable correctness issues rather than requiring every old sentence to hold simultaneously.

#### Spark outcome

The reviewed POM uses Spark **4.1.1** and Delta **4.1.0**, not Delta 4.0. Delta's official matrix supports that pairing. Gluten's native Delta integration has a separate compatibility and executed-plan validation requirement. See `spark-retrieval-review-codex.md` for the full official-source comparison.

Current Java collection already uses `executedPlan().executeCollect()` and maps `InternalRow[]` directly. It avoids the public external Row conversion of `collectAsList`. Its main remaining constraint is full result materialization and coexistence of collected rows with ResultFrame arrays. `toLocalIterator`/internal `executeToIterator` trade temporary memory for partition/job overhead; PySpark's Arrow setting does not accelerate this Java collector; JSON would introduce encoding/parsing work for a typed-frame consumer. There is no demonstrated universal 10x path. Measure collect, map, encoding and peak memory separately, then compare identical workloads before changing the collector.


<!-- END CONSOLIDATED SOURCE: docs/cache-python-parity-review-codex.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/freshness-review-codex.md -->
<a id="freshness-review"></a>

Source document: `docs/freshness-review-codex.md` (original encoding: utf-8).

> Cleanup note (18 October 2026): standalone review probes and generated `codex-*.log` / benchmark JSON artifacts referenced below were deleted after the untracked-file audit. References describe historical evidence, not files still present. Findings and recorded measurements remain historical; current verification is recorded in `UNCOMMITTED_COMMIT_PLAN.md`.

> Historical review snapshot. Fix status and final validation are superseded by [the 2026-10-16 fix review](#cache-core-fix-review).

### Cascada freshness review against the supplied baseline

> Source-parity correction after personally reading all 20 files in the original Python smart_cache directory: the sliding validation baseline below is already implemented in Python auto_warmer.py, and the failed-refresh acknowledgement problem can occur there too because per-bucket failures are swallowed before the baseline is saved. These are NOT evidence that Java alone introduced those behaviors. The frozen-epoch finding is a README-versus-both-implementations discrepancy. See `cache-python-parity-review-codex.md` for the final combined classification. This report's code observations and reproduced outputs remain valid.

Reviewed 2026-10-15, Europe/Paris. Source review only; no production files changed. The working tree is being edited concurrently, so findings describe the files read, not a clean commit. `review_claude.md` is excluded from evidence as requested.

Both baseline documents were read completely. Explicit normative sections take precedence over conflicting older examples: SMART_CACHE_README §8 says only the warmer writes bucket payloads, despite the older lazy-write diagram in §4; §4.4 specifies frozen validation epochs, despite the older always-overwrite table in §8. The technical details specify fixed 300-second storage steps and display resampling, despite older examples describing separate cache families for display steps. Pickle, Python classes and exact MD5 bytes are implementation details unless cross-language byte compatibility is required.

#### Confirmed findings

##### P1: The cube cache outlives bucket freshness

`CacheExecutionEngine.java:129-133` answers from `CubeShapeCatalog` before bucket lookup. `CubeShapeCatalog.java:84-88,95-103` stores frames and exact answers without timestamps, TTL or a source version. Registering an existing shape is a no-op. The completed-window guard prevents caching live windows, but does not invalidate completed history after backfills, TTL expiry or a disabled validation canary. Factory wiring always creates the catalog; validation SQL is optional.

Reproduction against compiled current classes: `docs/codex-review-probes/CubeFreshnessProbe.java`. An empty bucket backend, a completed historical SUM query, and a fake authoritative source initially returning 100 produce `first=[{SUM(bytes)=100}]`. Change source to 200 and repeat: `after_source_change_with_no_bucket_hits=[{SUM(bytes)=100}]; spark_calls=1`. No bucket was ever stored. Continued access can retain this answer indefinitely, independently of Redis expiry, until catalog eviction, clearing, process restart or successful change-triggered invalidation. The existing cube verifier checks the arithmetic of the cached input, not whether it matches current Delta data.

Required change: give every answer tier a common freshness/generation contract. Bound cube entries by TTL/source generation, invalidate on relevant bucket expiry/refresh, and replace stale registrations. This is necessary even with correct roll-up mathematics.

##### P1: Failed forced refreshes are acknowledged before they succeed

`DataChangeDetector.java:80-87` saves the changed baseline before `WarmingOrchestrator.java:159` starts rewriting buckets. If one pattern fails at Spark/Redis, `WarmingOrchestrator.java:197-202` merely votes its hash back into the queue. On the next successful canary comparison the source matches the already-saved baseline, so `forceOverwrite=false`. `WarmingOrchestrator.java:280-291` skips the existing stale bucket instead of retrying the failed overwrite. The same problem occurs on a fresh epoch if overwrite fails. A row-cap rejection also leaves an old existing value in place.

Trigger: warm value 100; change historical source to 200; canary detects change; fail that pattern's refresh once; run another cycle with unchanged 200 source. The requeued pattern now sees the old key as present and skips it. Stale data survives until expiry or another detected change. This is a control-flow finding; no end-to-end Spark failure-injection test was run.

Required change: retain a dirty generation/forced-refresh obligation until each affected pattern/bucket completes, or invalidate the affected keys before allowing EXISTS-skip again. Baseline advancement alone cannot represent refresh completion.

##### P2: Frozen validation epoch has been replaced by a sliding, perpetually renewed baseline

Baseline: SMART_CACHE_README lines 318-347 explicitly requires one query over the stored frozen window, no write on match, and natural TTL expiry as the only epoch reset. Current `DataChangeDetector.java:80-91` queries the old window and, if it differs from the current window, queries the new one too; then saves the new window even when no change occurred. Every successful match resets TTL.

`docs/codex-review-probes/EpochProbe.java` output:

```
first_changed=true
unchanged_changed=false
baseline_writes=2; validation_queries=3; final_window=TimeRange[startTimestampSeconds=86400, endTimestampSeconds=345599]
```

The baseline contract expects one initial save, two total validation queries, and the original frozen window after those two cycles. Existing `DataChangeDetectorTest.unchangedHistoryKeepsTheCacheAndSlidesTheBaselineForward` explicitly asserts the changed behavior, so green tests do not establish README compliance. The consequence is extra Spark validation work and loss of the baseline's natural periodic full-refresh mechanism; bucket TTL still works separately.

Required change: match on the stored window without saving; on mismatch update the signature while retaining that window; open a new current window only after absence/expiry. Preserve pending refresh work as described above.

#### Additional coverage and caveats

- A separately reproduced P2 schema bug exists for all-empty grouped AVG ingredients. `IncrementalFrameMerge.java:45-49` returns the empty ingredient frame before reconstruction. `docs/codex-review-probes/EmptyAverageProbe.java` produces `expected=[app, avg_x]; actual=[app, avg_x__cascada_avg_sum, avg_x__cascada_avg_count, avg_x__cascada_avg_type]`. Zero rows are valid cached hits, but their public schema still needs the requested AVG alias. The direct AVG miss path also invokes this incremental merge. Fix empty-schema reconstruction with sufficient input type metadata; invoking numeric reconstruction blindly may not infer the empty AVG decimal type correctly.

- The warmer now excludes live buckets, writes with TTL, preserves empty frames, and enforces a row cap; those mechanisms exist in the reviewed source.
- The read engine now uses a Clock and completed-bucket planning. Claims that it has no clock or always caches today are not true of this snapshot.
- `warmLookbackWindow` excludes the bucket immediately preceding the live bucket from the validation window, and for short lookbacks can skip validation entirely (lines 155-158). This differs from the supplied README's described current lookback window. It may be a late-arrival policy, but the user-visible freshness contract needs to state it; the excluded bucket can have been warmed already.
- Validation signatures stringify cells and join unescaped separators. SQL NULL and literal string `null` have the same representation, and embedded separator characters can collide. This matters if the advertised arbitrary-row/column canary result is used. Prefer typed length-prefixed canonical encoding.
- No attempt was made to repair the code during this review.

#### Verification

Targeted Maven run, Java 21, in-process Surefire with required module opens: DataChangeDetectorTest (6), WarmingFreshnessTest (5), FramePostProcessingTest (5), TypedMergeRegressionTest (16): **32 tests, zero failures/errors, Maven BUILD SUCCESS**. The command wrapper reported exit 1 even for that successful Maven output; this is recorded rather than presented as an entirely clean process exit. Earlier forked runs failed in Surefire setup; the first in-process attempt lacked Agrona's `jdk.internal.misc` open and was corrected for the successful run.

Probe sources compile with the cache and identity target classes and use fake ports; they are evidence for local control flow, not a real Spark/Valkey integration benchmark.

#### Snapshot identity

Git HEAD was `f9f0aac231507c0d533a705b10061cf90189a083` with extensive pre-existing uncommitted modifications. SHA-256 hashes after review:

| File | SHA-256 |
|---|---|
| CacheExecutionEngine.java | `9F25A755BE261B0DF48D9160C1638F6FDDF31B29460D83532BF8684A05763076` |
| WarmingOrchestrator.java | `DC42F4FB445A4F4F5F6A7CCC0D37BA437A9912F89FF7794F553E26E585F4E6D9` |
| DataChangeDetector.java | `5E61FB9F6872FDAE2E4FDB40F5C8C4B7327DD03E5E8507950328F19E027CFCC3` |
| SparkDeltaQueryExecutor.java | `7C781232175AF25A93A47BF96F8A8D4625B7EF24C2F9C8B1E2AA26B3B2329DE6` |


<!-- END CONSOLIDATED SOURCE: docs/freshness-review-codex.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/readability-performance-review.md -->
<a id="readability-performance-review"></a>

Source document: `docs/readability-performance-review.md` (original encoding: utf-8).

### Readability, streams, and logging review

Reviewed 17 October 2026. Priority: query speed and low memory use before shorter syntax.

#### Coverage

Four parallel reviewers covered SQL, cache, platform/configuration, and logging. The production
inventory and logging scan covered 239 Java files across all seven modules, eight Maven POMs,
and deployment resources. SQL review covered all 30 production files. Cache review enumerated
139 production files, examined every stream/toList/toArray site, and inspected merge, warming,
time-bucket, presence, and cube paths more closely. Platform review covered app, fabric, identity,
Spark, and Spark configuration. Generated classes, downloaded native sources, and test/probe
console output were excluded from production findings.

This was a static review plus regression testing, not a throughput or allocation benchmark.
The checkout already contained extensive changes; those were preserved.

#### Changes made

* `CascadaLauncher`: replace the one production println with the existing JUL logger and lazy
  startup message; reuse a static logger for shutdown failures. The startup message now follows
  logger configuration (the default ConsoleHandler writes to stderr).
* `WarmingOrchestrator`: guard two FINE messages before formatting, avoiding concatenation and
  capturing-supplier construction when FINE is disabled. Correct the skipped-bucket diagnostic:
  decimal composite aggregates can cause a skip even when the row cap was not exceeded.
* `GlutenRuntimeValidator`: take one local `sparkProperties()` snapshot. That accessor returns
  a defensive map copy; native startup previously requested up to five copies. Encapsulation and
  validation rules are unchanged.
* `SparkConfigurationFileReader`: recurse directly over nested maps instead of copying each
  section into a temporary LinkedHashMap. Preserve traversal and last-value-wins behavior.
  Add regression coverage for nested groups, duplicate settings, and invalid nested values.
* `performance-logging.properties`: point merge diagnostics at `IncrementalFrameMerge`, the
  class that actually emits them.

No query scanner, canonicalization, aggregation, serialization, or transport data loop was changed.
No dependencies, async logging queues, threads, or caches were added.

#### SQL: keep stateful loops

| Source | Why the loop remains appropriate |
| --- | --- |
| `SqlTextScanner`, `CalciteSql.quoteReservedTimestampIdentifiers` | Quote/comment masks, cursor advancement, escaping, and token boundaries are stateful. |
| `MySqlToSparkFunctionTranslator` | Parenthesis depth, argument boundaries, and Matcher append state need explicit sequencing. |
| `SqlTrees` recursive walkers | Depth-first visits, alias exclusion, and early exits are clearer without a pipeline per AST node. |
| `GapQueryBuilder` | Range coalescing carries the previous interval; other traversals mutate AST nodes or deduplicate projections. |
| `CalciteCanonicalObjectFactory`, `AggregateNormalizer` | Visitors collect multiple related outputs and preserve identity/alias behavior. |

`RegisteredTable` and `TimeDimensionMap` already use concise normalization streams. Existing
map/filter pipelines that return required lists or sets can remain. There was no compelling
loop-to-stream conversion that improved readability enough to justify changing the query path.
Do not introduce parallel streams into query processing without workload-specific evidence.

The same reasoning applies to primitive frame accumulation, Spark result mapping, direct JSON
and Parquet output, and bounded gRPC chunk copying. Keep reverse-order resource cleanup and
Fabric's parse-all-manifests-before-apply ordering.

#### Logging decision

After the launcher change, production `src/main` contains no System.out, System.err, or
printStackTrace calls. Cascada uses java.util.logging in 11 classes. Existing Spark, Valkey,
and merge timing messages already gate timing and formatting with FINE checks. Warning/error
messages remain on failure paths. HTTP writes and gRPC sends are response payloads.

The parent POM manages an slf4j-api version; dependencyManagement alone does not install a
provider. Spark's deployment Log4j2 configuration does not configure Cascada's JUL loggers.
Test runs report missing SLF4J providers; the deployed Spark runtime's provider must be checked
in that runtime before drawing a production conclusion.

Keep JUL for this change: the source shows sparse diagnostics, not per-row logging, and there
is no measured logging bottleneck. Log4j2 async logging may help a logging-heavy workload,
but it adds a ring buffer/background processing and a queue-full policy. It is not a free
memory or latency improvement. A migration should compare request latency, allocation, queue
memory, shutdown flushing, and saturated-output behavior on the deployed runtime.

References: [Log4j async logging](https://logging.apache.org/log4j/2.x/manual/async.html),
[Java 21 streams](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/stream/Stream.html).

#### Follow-up candidates left unchanged

These require separate behavior decisions or measured changes; they are not claimed speedups.

* `app/.../concurrent/RequestExecutor`: virtual-thread-per-task execution has no application
  concurrency cap, and ServerSettings has no admission-limit setting. Simultaneous queries can
  multiply retained result memory even though chunks are bounded. Decide the overload policy
  and deployment-wide memory budget before adding admission control.
* `InMemoryBlobCacheBackendAdapter.sizeReport`: collects all live bucket byte-array references
  before summing. Running byte/count totals could remove an O(bucket-count) temporary reference
  list. This is the local/test backend, not the production Valkey sizing path.
* `DataChangeDetector.signature`: creates a SHA-256 digest per row and repeatedly resolves
  column indexes/null checks. Investigate digest reuse and prepared indexes while preserving
  exact signature bytes, ordering, nulls, and baseline compatibility.
* `CubeFilterEvaluator` and cube roll-up/verification: prepare predicates and column indexes
  once rather than reparsing filters per row. Preserve numeric, decimal, NULL, and IN semantics;
  measure whether lightweight row-view wrappers escape before replacing them.
* `ShapeLatticeIndex`: candidate sorting followed by projection creates another reference list.
  Review its ordering and admission semantics before consolidating collections.
* `TimeBucketCalculator.getCompletedBuckets`: an ordered prefix can be identified without two
  streams. Also review `nowSeconds - secondsPerBucket` at Long.MIN_VALUE for underflow; cover
  extreme timestamps before changing the comparison.

For any later hot-path refactor, compare equivalent workloads using the existing benchmark
tools, including allocation/GC and peak memory as well as latency. Correctness tests alone do
not establish performance equivalence.

#### Validation

* `scripts/Test-LocalSparkConsistency.ps1 -UnitTests -HeapMb 1536`: reactor BUILD SUCCESS across
  all seven modules. SQL reported 193 tests with no failures/errors. Environment-gated Spark,
  Valkey, and large-result integration tests were skipped; native/Linux behavior was not exercised.
* After the configuration changes, reran the script with
  `-TestClass 'SparkConfigurationFileReaderTest,GlutenRuntimeValidatorTest' -HeapMb 1536`:
  BUILD SUCCESS, six tests passed, none skipped.
* Production console-print scan found no matches. Whitespace diff checks passed for the tracked
  files changed here. No speedup or peak-memory benchmark claim is made.


<!-- END CONSOLIDATED SOURCE: docs/readability-performance-review.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/spark-retrieval-review-codex.md -->
<a id="spark-retrieval-review"></a>

Source document: `docs/spark-retrieval-review-codex.md` (original encoding: utf-8).

### Spark result retrieval review

**Reviewed:** 2026-10-15
**Scope:** Spark 4.1.1 / Delta Lake 4.1.0 compatibility, the current `lib_spark` result path, and Java-side alternatives for moving query results from Spark to Cascada.

#### Findings

The Maven versions are aligned for the ordinary Spark + Delta combination: Spark 4.1.1, Scala 2.13.17, and Delta 4.1.0 using `delta-spark_4.1_2.13`. Delta's official compatibility table says Delta 4.1.x supports Spark 4.0.x and 4.1.x, and the 4.1.0 release notes identify the Spark 4.1 artifact family. That establishes the Spark/Delta pairing. It does not by itself establish native Delta offload in Gluten: the upstream Gluten Delta support page still lists Spark 3.5/Delta 3.3 and Spark 4.0/Delta 4.0, but no Spark 4.1/Delta 4.1 row. This repository pins and builds a custom Gluten bundle and has a separate native Delta validation gate, so the exact bundle should be accepted only after that gate passes on the intended runtime.

The current Cascada collector is already more direct than `Dataset.collectAsList()`: it enters Spark's classic execution plan, calls `executeCollect()` to get Spark `InternalRow`s, and maps primitive values straight into `ResultFrame` columns. This skips Spark's public conversion of every row into an external `Row`. The main cost left is result size and peak driver memory. `executeCollect()` gathers every partition's encoded output, decodes it into an `InternalRow[]`, then the mapper allocates the final typed arrays while that row array remains live. The `ResultFrame` contract then retains the entire result. A different collection call cannot make that contract bounded-memory.

There is no source evidence for a 10x improvement from switching retrieval methods. The opt-in 55-million-row integration path times only the current collector plus JSON encoding; the 10 GB Delta benchmark times end-to-end Spark queries and collects their small aggregate outputs with `collectAsList()`. Neither compares the current `InternalRow` path with `toLocalIterator`, Arrow, JSON, or columnar extraction.

#### Compatibility and native runtime

The root [pom.xml](pom.xml#L29) sets Java 21, Spark 4.1.1, Scala 2.13.17, Delta 4.1.0, and Arrow Java 18.3.0. [lib_spark/pom.xml](lib_spark/pom.xml#L34) uses `spark-sql_2.13` and `spark-hive_2.13` 4.1.1 plus the matching `delta-spark_4.1_2.13` 4.1.0 artifact, all as `provided` dependencies supplied by the runtime image.

The official [Delta compatibility matrix](https://docs.delta.io/releases/) lists Delta 4.1.x with Spark 4.1.x. Delta's [4.1.0 release notes](https://github.com/delta-io/delta/releases/tag/v4.1.0) describe the Spark 4.1.0 and Spark 4.0.1 builds and the version-suffixed Maven artifacts. Spark 4.1.1 itself supports Java 17/21 and Scala 2.13 according to the [versioned Spark documentation](https://spark.apache.org/docs/4.1.1/).

The Gluten layer needs its own compatibility check. This repository's [bundle script](scripts/Build-GlutenDeltaBundle.ps1) pins Gluten commit `52f7cc9d3d2536cead21e95a6dc28f36923b7dd6` and builds with `-Pdelta -Pspark-4.1`. The pinned [Gluten Velox Delta support matrix](https://github.com/apache/gluten/blob/52f7cc9d3d2536cead21e95a6dc28f36923b7dd6/docs/get-started/VeloxDelta.md#L3-L20) only declares Spark 3.5/Delta 3.3 and Spark 4.0/Delta 4.0. Current upstream Gluten documentation lists Spark 4.1.1 as a supported Velox Spark version, but its [Delta support matrix](https://github.com/apache/gluten/blob/main/docs/get-started/VeloxDelta.md#L3-L20) still omits the Spark 4.1/Delta 4.1 pair. Treat the repository's custom combination as locally validated support, not as a pairing guaranteed by the published Gluten matrix.

The repository already has the right sort of functional gate: [NativeDeltaValidation.java](app/src/test/java/com/cascada/app/bootstrap/NativeDeltaValidation.java) checks the active `velox-delta` component, checks the executed plans for `DeltaScanTransformer`, and compares Delta reads and transaction semantics against a JVM-only Spark session. [Complete-GlutenDeltaValidation.ps1](scripts/Complete-GlutenDeltaValidation.ps1) runs that smoke gate before its broader checks. The bundle validator's class and metadata checks are useful startup checks, but those checks alone do not prove that a Delta query was offloaded. This review read the sources and did not run the native gate.

#### What the current result path does

`SparkDeltaQueryExecutor.executeSql()` creates the lazy `Dataset<Row>` and calls `toResultFrame()`. That method casts to `org.apache.spark.sql.classic.Dataset`, opens an SQL execution id, then calls `queryExecution.executedPlan().executeCollect()` ([SparkDeltaQueryExecutor.java](lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SparkDeltaQueryExecutor.java#L84-L107)). This is explicitly a Spark Classic implementation detail, rather than a stable `Dataset` Java API.

In the exact Spark 4.1.1 source, `SparkPlan.executeCollect()` calls `getByteArrayRdd().collect()`, decodes all partition buffers into an `ArrayBuffer[InternalRow]`, and returns an array. The same source has a separate `executeToIterator()` path based on `RDD.toLocalIterator`; Spark comments that this path triggers multiple jobs, one per partition. `executeCollectPublic()` adds a Catalyst conversion from the internal rows to external `Row`s ([Spark 4.1.1 `SparkPlan.scala`](https://github.com/apache/spark/blob/v4.1.1/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala#L338-L451)).

The mapper avoids the external-row conversion for primitive numerics and booleans; it reads directly from each `InternalRow` and appends into typed columns. Strings are copied from Spark UTF-8 values to Java `String`s, decimals become `BigDecimal`s, and date/time/null values use Spark's external type converter ([SparkResultFrameMapper.java](lib_spark/src/main/java/com/cascada/spark/adapter/out/spark/SparkResultFrameMapper.java#L32-L79)). `expectedRows(rows.length)` lets `ResultFrameBuilder` allocate final arrays at the exact known size, avoiding geometric growth. At build time it keeps those column arrays and only clones the small outer arrays ([ResultFrameBuilder.java](lib_cache/src/main/java/com/cascada/cache/domain/frame/ResultFrameBuilder.java#L20-L26), [build path](lib_cache/src/main/java/com/cascada/cache/domain/frame/ResultFrameBuilder.java#L110-L130)).

The resulting memory profile is therefore:

1. Spark tasks encode their output as per-partition byte buffers.
2. `executeCollect()` collects those buffers on the driver and decodes them into row objects backed by per-row byte arrays.
3. The mapper builds the complete `ResultFrame` column arrays and any Java strings/decimals.
4. The frame stays resident because `QueryExecutorPort.execute()` returns one complete `ResultFrame` ([port](lib_cache/src/main/java/com/cascada/cache/application/port/out/QueryExecutorPort.java#L10-L18)).

This is a good throughput-oriented route for bounded results: it uses one parallel Spark collection job and avoids external `Row` objects. For large results, the `InternalRow[]` and final frame coexist during mapping, and per-cell string/decimal conversion adds allocation. `spark.driver.maxResultSize` is set to 6 GiB in the default infrastructure settings ([StaticSparkInfrastructureSettings.java](lib_spark_config/src/main/java/com/cascada/sparkconfig/application/service/StaticSparkInfrastructureSettings.java#L87-L99)); Spark documents that this setting caps serialized task results and that a high limit can still permit driver OOM due to JVM object overhead ([Spark 4.1.1 configuration](https://spark.apache.org/docs/4.1.1/configuration.html#application-properties)). It is a guardrail, not a bound on the expanded `ResultFrame` heap footprint. The large-result smoke test disables it with `0` and checks 55 million `range` rows, which is useful for capacity testing but is not a safe production limit ([LargeResultIntegrationTest.java](app/src/test/java/com/cascada/app/bootstrap/LargeResultIntegrationTest.java#L10-L27)).

One measurement gap is visible in the production logger: it reports `execute_and_map_ms` for the combined collect-plus-map interval, so it cannot tell whether Spark transport or Java conversion dominates. Split those timings and record result bytes, row count, column types, GC time, and peak driver heap before optimizing the path.

#### Retrieval choices

| Path | Expected behavior here | Assessment |
|---|---|---|
| `Dataset.collectAsList()` | Spark collects all rows to the driver, converts them to external `Row`s, stores an array, then wraps it as a Java list. Spark's 4.1.1 implementation calls `collectFromPlan()` and then `Arrays.asList`; it does not stream ([Dataset.scala](https://github.com/apache/spark/blob/v4.1.1/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L1396-L1401)). | Easy public API, but not a faster replacement. It adds row conversion/allocation to the current path and retains the complete result. Spark warns that collecting all data can OOM the driver ([Dataset JavaDoc](https://spark.apache.org/docs/4.1.1/api/java/org/apache/spark/sql/Dataset.html#collectAsList())). |
| `Dataset.toLocalIterator()` | Public iterator; documented peak collection memory is the largest partition. It triggers multiple Spark jobs and may recompute a wide query once per partition unless cached ([Dataset JavaDoc](https://spark.apache.org/docs/4.1.1/api/java/org/apache/spark/sql/Dataset.html#toLocalIterator())). | Useful when driver peak memory from Spark's intermediate collection is the limiting factor, at the cost of executor recomputation/job overhead. For `Dataset<Row>` it still produces external rows. It cannot remove the complete-frame memory requirement. |
| Classic plan `executeToIterator()` | Spark 4.1.1 internal path streams decoded `InternalRow`s from partition buffers and avoids building `InternalRow[]`; it has the same multiple-job caveat as local iteration ([SparkPlan source](https://github.com/apache/spark/blob/v4.1.1/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkPlan.scala#L438-L445)). | Most direct streaming experiment for Cascada because it preserves internal-row mapping. It is version-coupled and needs a builder that handles unknown row counts. Benchmark it before adopting; it may trade lower temporary heap for more jobs and recomputation. |
| Arrow Java / PySpark Arrow | The Java `Dataset` API has `collectAsList`, `toJSON`, and `toLocalIterator`, but no public Java `toArrow()` result method ([Spark JavaDoc](https://spark.apache.org/docs/4.1.1/api/java/org/apache/spark/sql/Dataset.html)). PySpark 4.1.1 has `DataFrame.toArrow()`, but calls it a developer API and says the full table is loaded into driver memory ([PySpark `toArrow`](https://spark.apache.org/docs/4.1.1/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toArrow.html)). The `spark.sql.execution.arrow.pyspark.enabled` setting applies to PySpark `toPandas` and Pandas/NumPy DataFrame creation, not this Java collector ([Spark configuration](https://spark.apache.org/docs/4.1.1/configuration.html#spark-sql)). | Arrow IPC could be tested as a serialized partition payload, but there is no public Java `Dataset` fast path to turn on. The root POM's Arrow 18.3.0 is used by the cache serialization module (`lib_cache/pom.xml`), not declared by `lib_spark/pom.xml`; it does not make Spark query results Arrow-backed. |
| Gluten / Spark columnar batches | Gluten can run supported plan sections with Velox and use columnar shuffle. Spark has separate row and columnar physical-plan execution methods; the current `executeCollect()` path consumes `InternalRow`s through `execute()`, so it does not directly hand the driver's mapper a native batch. Gluten's plan guide shows `VeloxColumnarToRowExec` as a boundary when a native columnar plan must return to rows ([Gluten Velox guide](https://github.com/apache/gluten/blob/main/docs/get-started/Velox.md#L373-L407)). | Gluten can reduce time spent in supported scan/filter/aggregation/shuffle operators, but it does not remove the final driver collection and frame materialization. Calling internal `executeColumnar()` and retaining `ColumnarBatch` objects on the driver would need explicit batch ownership, native-memory cleanup, serialization, unsupported-operator, SQL metrics, and cancellation handling. Treat that as a prototype, not a drop-in Java API. |
| `Dataset.toJSON()` | Returns a `Dataset<String>` and generates one JSON string per row in a partition ([Spark 4.1.1 source](https://github.com/apache/spark/blob/v4.1.1/sql/core/src/main/scala/org/apache/spark/sql/classic/Dataset.scala#L1573-L1598)). | Appropriate only if JSON strings are the final output and can be streamed directly to a consumer. For Cascada's typed `ResultFrame`, it adds JSON encoding and then requires parsing or loses typed cache semantics. Collecting those strings still loads the result on the driver. |
| `foreachPartition` / executor-side batches | Performs work at the executors and can write partition results to a sink. | The only route in this list that can avoid collecting all data into the driver, but it changes the contract: the port currently returns a full frame. A real bounded-memory design must stream partition batches into a consumer/cache sink or change the result abstraction. |
| Spark Connect | Spark Connect returns results to clients as Arrow-encoded row batches over gRPC ([Spark 4.1.1 Connect overview](https://spark.apache.org/docs/4.1.1/spark-connect-overview.html#how-spark-connect-works)). It isolates the client from the driver and removes client-side access to SparkContext and the driver JVM. | A meaningful architecture option for process isolation and streamed transport, not a local switch for the current fast path. The cast to `classic.Dataset`, direct `queryExecution` access, job-group cancellation, and RDD/plan hooks must all be redesigned. The client still builds a complete `ResultFrame`, so the current return contract remains linear in total output size. Delta Connect is documented as preview and not recommended for production in the [Delta Connect guide](https://docs.delta.io/delta-spark-connect/). |

#### Recommendations

1. **Keep the current direct `executeCollect()` mapper for bounded results.** Replacing it with `collectAsList()` would reintroduce external Spark rows and list storage. The existing `InternalRow` access is already a sensible Java fast path for the current typed frame.
2. **Make the result-size contract explicit.** Keep a meaningful `spark.driver.maxResultSize` for normal deployments and add a request-level row/byte policy or a streaming result contract for workloads that must exceed it. Raising the cap alone only moves the failure point.
3. **If driver peak heap is the measured problem, prototype `executeToIterator()` behind a selectable path.** Consume and map each row before advancing the iterator, retain the SQL execution id and cancellation scope for the entire consumption, and address unknown-size builder growth. Compare it against the current path on narrow numeric and wide string/decimal results, with narrow and wide Spark plans. The Spark documentation predicts a memory/recomputation trade-off, not a universal speed increase.
4. **If Java row conversion is the measured problem, prototype executor-side Arrow IPC batches.** Have each partition encode bounded Arrow IPC payloads and stream those payloads to a consumer; measure executor conversion cost, network bytes, driver decode cost, peak heap, and native-memory cleanup. Simply calling PySpark `toArrow()` does not provide this Java path and still collects the entire table.
5. **If total result size is the problem, change the frame boundary.** A batch or partition consumer can let downstream cache merge/serialization process data incrementally. With `QueryExecutorPort.execute()` returning one full `ResultFrame`, every retrieval method must ultimately keep the answer in driver memory.
6. **Treat the Gluten + Delta combination as a custom validated runtime.** Keep the repository's native Delta scan gate in the release process and record successful run evidence for the exact Spark 4.1.1 / Scala 2.13 / Delta 4.1.0 / Gluten commit and bundle. Presence of classes in a JAR is not a substitute for observing `DeltaScanTransformer` in an executed plan.
7. **Correct the Arrow configuration label.** `SparkConfigurationDeriver` calls `spark.sql.execution.arrow.pyspark.enabled` “Arrow always on”; Spark 4.1.1 defines that switch for PySpark conversion. It has no effect on `SparkDeltaQueryExecutor`'s Java `executeCollect()` path. Rename/remove the setting unless the deployment separately runs PySpark workloads.

#### Benchmark needed before making a speed claim

Use the same Spark/Delta/Gluten image, physical SQL, input snapshot, executor count, AQE settings, and output schema for each candidate. Compare:

- current direct internal collect + mapper;
- public `collectAsList()` plus equivalent typed mapping;
- public `toLocalIterator()` and, separately, internal `executeToIterator()`;
- an executor-encoded Arrow IPC batch path;
- `toJSON()` only when the endpoint consumes JSON directly.

Include small aggregate outputs, large primitive outputs, and wide string/decimal outputs. Run warm and cold trials, verify result schemas and hashes, and report median end-to-end time, rows/s, task result bytes, driver peak heap/RSS, GC pause/time, and executor time. For Gluten cases, capture the executed plan and count native `DeltaScanTransformer` nodes so a fallback run is not reported as native. The existing [55-million-row smoke test](app/src/test/java/com/cascada/app/bootstrap/LargeResultIntegrationTest.java#L10-L27) checks one collection path; the existing [10 GB benchmark](app/src/test/java/com/cascada/app/bootstrap/DeltaTenGbBenchmark.java#L150-L180) does not compare these retrieval strategies. No speedup multiplier is asserted here.


<!-- END CONSOLIDATED SOURCE: docs/spark-retrieval-review-codex.md -->

<!-- BEGIN CONSOLIDATED SOURCE: docs/sql-merge-review-codex.md -->
<a id="sql-merge-review"></a>

Source document: `docs/sql-merge-review-codex.md` (original encoding: utf-8).

### SQL and merge review

Scope: current Cascada SQL canonicalization, gap-query rewriting, AVG reconstruction, time-series merge, and deferred post-processing. I read both supplied Smart Cache baseline documents in full. I did not consult `review_claude.md` and made no source-code changes.

#### Confirmed supported-path finding

**P2 — Scalar `AVG(DECIMAL)` over an empty input returns a DOUBLE-typed result.**

Trigger: an accepted, explicitly aliased global average such as `SELECT AVG(amount) AS mean FROM events WHERE ts >= 0 AND ts <= 86399`, where `amount` is DECIMAL and the window has no rows. `GapQueryBuilder.replaceAverages()` rewrites this to `SUM(amount)`, `COUNT(amount)`, and `MIN(TYPEOF(amount))` at [GapQueryBuilder.java:127](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryBuilder.java:127). For a scalar aggregate Spark still returns one row, but the type aggregate is NULL on empty input. `SparkAverage.inputType()` then defaults to `"double"` at [SparkAverage.java:126](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/merge/SparkAverage.java:126), and `resultType()` emits `ColumnType.DOUBLE` at [SparkAverage.java:133](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/merge/SparkAverage.java:133). The value remains NULL, but the returned schema differs from the original DECIMAL AVG result type.

Requirements mapping: the baseline requires AVG to be reconstructed from merged SUM/COUNT ingredients ([`cashing_system_technical_details.md` §8, lines 391–395](C:/Users/omarm/Downloads/dsitrbuted_engine/data_collector/src/cashing_system_technical_details.md:391); [`SMART_CACHE_README.md` §3, lines 181–185](C:/Users/omarm/Downloads/dsitrbuted_engine/data_collector/src/SMART_CACHE_README.md:181)). Those sections focus on the value; Cascada's `SparkAverage` additionally promises Spark-compatible result types in its class contract at [SparkAverage.java:20](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/merge/SparkAverage.java:20). The empty-input branch violates that contract.

The primary review has independently found the separate empty *grouped* AVG schema leak: `IncrementalFrameMerge.finish()` returns the first empty ingredient frame before `finishMergedFrame()` can rename its internal columns ([IncrementalFrameMerge.java:45](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/IncrementalFrameMerge.java:45), [FrameMergeService.java:90](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/FrameMergeService.java:90)). I confirmed this path and am not duplicating it as a separate finding here.

#### Conservative bypasses: capability limits, not wrong cached answers

The factory marks projections it cannot safely merge with `UNSUPPORTED_MERGE_SHAPE` ([CalciteCanonicalObjectFactory.java:85](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/CalciteCanonicalObjectFactory.java:85)); `NonMergeableSqlFeatureRule` then bypasses any query with a non-empty logic signature ([NonMergeableSqlFeatureRule.java:33](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/safety/NonMergeableSqlFeatureRule.java:33)). Consequently, unaliased AVG, composite aggregate projections such as `SUM(a) + SUM(b) AS total`, and expression-based `ORDER BY` do not use the cache. They execute through the direct-query path and should be treated as cache-capability gaps relative to the baseline examples/utilities, not as incorrect answers from a cached merge.

#### Areas reviewed

- Canonicalization sorts group keys and normalized AVG ingredients, extracts time bounds from supported integer comparisons/BETWEEN, and keeps time-window, post-processing, and logic metadata separate ([CalciteCanonicalObjectFactory.java:73](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/CalciteCanonicalObjectFactory.java:73), [TimeBoundClassifier.java:14](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/TimeBoundClassifier.java:14)).
- Gap rewriting coalesces adjacent missing ranges and injects their predicate after dropping recognized time-only conjuncts; it removes top-level ordering/limit from the fetch query ([GapQueryBuilder.java:57](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryBuilder.java:57), [GapQueryBuilder.java:87](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryBuilder.java:87), [GapQueryBuilder.java:241](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_sql/src/main/java/com/cascada/sql/adapter/out/calcite/GapQueryBuilder.java:241)).
- For cacheable AVG aliases, typed merge combines SUM and COUNT ingredients before `SparkAverage.finish()` reconstructs the average; deferred column ORDER BY and LIMIT run afterward ([FrameMergeService.java:90](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/FrameMergeService.java:90), [FrameMergeService.java:323](C:/Users/omarm/Downloads/dsitrbuted_engine/cascada/lib_cache/src/main/java/com/cascada/cache/application/service/FrameMergeService.java:323)). I found no additional confirmed defect in the normal non-empty path from this source review.

The baseline docs disagree on time-step identity: the technical details describe one fixed cache step with display resampling (§7, lines 363–367), while the README says step is included in the hash and different granularities get different keys (§2, lines 139–150; §13 example 9, lines 988–999). I therefore did not classify step-key behavior as a bug.

Validation: source trace only. I did not run Maven because the primary agent was running the shared test targets concurrently.


<!-- END CONSOLIDATED SOURCE: docs/sql-merge-review-codex.md -->


<!-- BEGIN CONSOLIDATED SOURCE: docs/benchmarks/merge-redesign-2026-10-18/REPORT.md -->
<a id="merge-redesign-measurements"></a>

Historical benchmark record, preserved in full. Paths in this record are relative to `docs/benchmarks/merge-redesign-2026-10-18/`.

### Cascada merge redesign: before/after benchmark, 18 October 2026

Same workload as `avg20m-12cols-2026-10-18`: **20,000,000 partial-aggregate rows × 12 columns per query**, 300 two-hour buckets, GROUP BY 3 keys, 3 weighted AVG outputs. Machine: AMD Ryzen 7 5700G (8 cores / 16 threads), 31 GiB, Windows 11, OpenJDK 21.0.12, G1, `-Xms1g -Xmx14g`. Separate JVM per case, one warm-up, then measured rounds. **Every measured round re-verifies every output group against the independent oracle (all PASS).**

- **Before** = current production `FrameMergeService.incremental` → `TypedFrameAccumulator` → `TypedAggregationGroups`.
- **After** = prototype `FastAvgMerge`/`GroupTable` (in `prototype/`): row-major paged group state, hash-tagged slots, column-at-a-time frame processing, dictionary ids for `MIN(typeof)` strings, fused AVG finish, optional hash-partitioned workers (w = threads per query).
- `seq` = generator's ascending group order (the original report). `shuffled` = all 20M rows globally shuffled, so every bucket holds groups in arbitrary order (realistic Spark output).
- Heap = peak sampled heap above the retained input (merge working memory). CPU = process CPU-seconds for the round.

#### Summary (median wall time)

| Queries | Groups/query | Keys | Order | Before | After 1 thread | After 4 threads/query | After 8 threads/query | Merge heap before → after (1 thread) |
|---:|---:|---|---|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 3.69 s (**2.5×**) | 1.74 s (**5.3×**) | 1.60 s (**5.8×**) | 0.57 → 0.22 GiB (−62%) |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 10.66 s (**2.1×**) | 3.48 s (**6.5×**) | 2.65 s (**8.5×**) | 0.58 → 0.23 GiB (−61%) |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 14.25 s (**1.8×**) | 4.93 s (**5.1×**) | — | 0.56 → 0.22 GiB (−61%) |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 14.21 s (**2.2×**) | 5.52 s (**5.7×**) | 4.76 s (**6.6×**) | 2.61 → 1.17 GiB (−55%) |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 14.72 s (**1.9×**) | 9.30 s (**3.1×**) | — | 2.21 → 0.91 GiB (−59%) |
| 4 | 5,000,000 | LONG | shuffled | 39.87 s | 20.52 s (**1.9×**) | 13.59 s (**2.9×**) | — | 7.47 → 4.70 GiB (−37%) |

#### Every measured round

| Queries | Groups | Keys | Order | Version | Threads/query | Round | Wall | Input M rows/s | Process CPU-s | Avg cores | Merge heap GiB | Verify |
|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | before | 1 | 0 | 9.925 s | 2.02 | 10.0 | 1.01 | 0.570 | PASS |
| 1 | 1,000,000 | LONG | seq | before | 1 | 1 | 9.270 s | 2.16 | 9.7 | 1.04 | 0.539 | PASS |
| 1 | 1,000,000 | LONG | seq | before | 1 | 2 | 9.280 s | 2.16 | 9.2 | 0.99 | 0.539 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 0 | 3.763 s | 5.31 | 3.9 | 1.04 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 1 | 3.687 s | 5.42 | 3.8 | 1.03 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 1 | 2 | 3.682 s | 5.43 | 3.8 | 1.02 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 0 | 1.880 s | 10.64 | 6.9 | 3.68 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 1 | 1.726 s | 11.59 | 6.5 | 3.77 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 4 | 2 | 1.744 s | 11.47 | 6.7 | 3.83 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 0 | 1.792 s | 11.16 | 11.6 | 6.48 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 1 | 1.598 s | 12.52 | 11.6 | 7.25 | 0.211 | PASS |
| 1 | 1,000,000 | LONG | seq | after | 8 | 2 | 1.520 s | 13.16 | 10.5 | 6.92 | 0.211 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 0 | 22.556 s | 0.89 | 22.7 | 1.01 | 0.578 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 1 | 22.858 s | 0.87 | 23.1 | 1.01 | 0.570 | PASS |
| 1 | 1,000,000 | LONG | shuffled | before | 1 | 2 | 22.314 s | 0.90 | 22.3 | 1.00 | 0.547 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 10.870 s | 1.84 | 11.0 | 1.01 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 10.662 s | 1.88 | 11.0 | 1.03 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 10.659 s | 1.88 | 10.6 | 1.00 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 0 | 3.417 s | 5.85 | 12.8 | 3.73 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 1 | 3.486 s | 5.74 | 12.7 | 3.65 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 4 | 2 | 3.478 s | 5.75 | 12.6 | 3.61 | 0.227 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 0 | 2.653 s | 7.54 | 18.3 | 6.90 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 1 | 2.861 s | 6.99 | 18.5 | 6.47 | 0.219 | PASS |
| 1 | 1,000,000 | LONG | shuffled | after | 8 | 2 | 2.634 s | 7.59 | 18.4 | 6.99 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 0 | 25.330 s | 0.79 | 25.9 | 1.02 | 0.555 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 1 | 31.208 s | 0.64 | 30.8 | 0.99 | 0.555 | PASS |
| 1 | 1,000,000 | STRING | shuffled | before | 1 | 2 | 24.553 s | 0.81 | 24.7 | 1.00 | 0.523 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 0 | 14.250 s | 1.40 | 14.5 | 1.01 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 1 | 14.246 s | 1.40 | 14.3 | 1.00 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 1 | 2 | 14.084 s | 1.42 | 14.2 | 1.01 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 0 | 4.875 s | 4.10 | 17.9 | 3.67 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 1 | 4.954 s | 4.04 | 18.4 | 3.71 | 0.219 | PASS |
| 1 | 1,000,000 | STRING | shuffled | after | 4 | 2 | 4.934 s | 4.05 | 18.4 | 3.74 | 0.219 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 0 | 34.854 s | 0.57 | 35.2 | 1.01 | 2.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 1 | 31.283 s | 0.64 | 31.6 | 1.01 | 2.609 | PASS |
| 1 | 5,000,000 | LONG | shuffled | before | 1 | 2 | 31.086 s | 0.64 | 31.0 | 1.00 | 2.438 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 0 | 14.439 s | 1.39 | 14.6 | 1.01 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 1 | 14.208 s | 1.41 | 14.5 | 1.02 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 1 | 2 | 14.042 s | 1.42 | 14.0 | 0.99 | 1.172 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 0 | 5.541 s | 3.61 | 17.3 | 3.13 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 1 | 5.453 s | 3.67 | 16.9 | 3.09 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 4 | 2 | 5.521 s | 3.62 | 17.5 | 3.18 | 1.258 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 0 | 4.758 s | 4.20 | 24.2 | 5.08 | 1.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 1 | 4.724 s | 4.23 | 24.2 | 5.13 | 1.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | after | 8 | 2 | 4.988 s | 4.01 | 26.0 | 5.22 | 1.297 | PASS |
| 4 | 1,000,000 | LONG | shuffled | before | 1 | 0 | 28.717 s | 2.79 | 112.3 | 3.91 | 2.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | before | 1 | 1 | 28.566 s | 2.80 | 110.6 | 3.87 | 2.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 14.654 s | 5.46 | 56.4 | 3.85 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 14.579 s | 5.49 | 56.2 | 3.85 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 14.541 s | 5.50 | 56.1 | 3.86 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 0 | 14.780 s | 5.41 | 57.5 | 3.89 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 1 | 14.930 s | 5.36 | 57.7 | 3.86 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 1 | 2 | 14.796 s | 5.41 | 57.2 | 3.87 | 0.906 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 4 | 0 | 9.361 s | 8.55 | 113.7 | 12.14 | 0.930 | PASS |
| 4 | 1,000,000 | LONG | shuffled | after | 4 | 1 | 9.231 s | 8.67 | 113.3 | 12.27 | 0.930 | PASS |
| 4 | 5,000,000 | LONG | shuffled | before | 1 | 0 | 40.059 s | 2.00 | 187.7 | 4.69 | 7.459 | PASS |
| 4 | 5,000,000 | LONG | shuffled | before | 1 | 1 | 39.683 s | 2.02 | 185.0 | 4.66 | 7.466 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 1 | 0 | 20.511 s | 3.90 | 81.7 | 3.98 | 4.695 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 1 | 1 | 20.526 s | 3.90 | 80.6 | 3.92 | 4.695 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 4 | 0 | 13.599 s | 5.88 | 152.9 | 11.24 | 5.055 | PASS |
| 4 | 5,000,000 | LONG | shuffled | after | 4 | 1 | 13.574 s | 5.89 | 153.2 | 11.28 | 5.055 | PASS |

#### Production code after the in-place rewrite (single thread, same harness)

`TypedAggregationGroups` was rewritten in place (row-major paged state, tagged slots, column-at-a-time frames, dictionary ids for STRING MIN/MAX) and `SparkValues.compareStrings` gained an equal-strings fast path. This step keeps the existing finish path (12-column merged frame, then `SparkAverage.finish`). It has no fused finish and no partitioned workers yet, which is why its memory and speed gains are smaller than the prototype's.

| Queries | Groups | Keys | Order | Before median | After median | Speedup | Process CPU-s before → after | Merge heap before → after | Verify |
|---:|---:|---|---|---:|---:|---:|---|---|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 3.91 s | **2.38×** | 9.7 → 4.2 | 0.57 → 0.40 GiB (−30%) | {'PASS'} |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 10.78 s | **2.09×** | 22.7 → 10.8 | 0.58 → 0.41 GiB (−30%) | {'PASS'} |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 15.55 s | **1.63×** | 25.9 → 15.8 | 0.56 → 0.39 GiB (−30%) | {'PASS'} |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 18.01 s | **1.74×** | 31.6 → 19.4 | 2.61 → 1.87 GiB (−28%) | {'PASS'} |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 16.09 s | **1.78×** | 111.4 → 63.8 | 2.21 → 1.63 GiB (−26%) | {'PASS'} |

Every round after the rewrite:

| Queries | Groups | Keys | Order | Round | Wall | Accept | Process CPU-s | Merge heap GiB | Verify |
|---:|---:|---|---|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 0 | 3.941 s | 3.433 s | 4.2 | 0.398 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 3.887 s | 3.416 s | 4.2 | 0.398 | PASS |
| 1 | 1,000,000 | LONG | seq | 2 | 3.906 s | 3.446 s | 3.8 | 0.367 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 0 | 10.632 s | 9.356 s | 10.8 | 0.406 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 10.811 s | 9.313 s | 11.2 | 0.375 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 2 | 10.782 s | 9.472 s | 10.7 | 0.375 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 0 | 15.757 s | 12.382 s | 16.0 | 0.391 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 15.547 s | 12.214 s | 15.8 | 0.391 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 2 | 15.457 s | 12.205 s | 15.4 | 0.359 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 0 | 19.630 s | 10.546 s | 21.0 | 1.874 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 18.007 s | 10.524 s | 19.4 | 1.872 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 2 | 17.751 s | 10.454 s | 18.1 | 1.724 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 0 | 16.172 s | 13.953 s | 64.8 | 1.633 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 16.010 s | 13.910 s | 62.9 | 1.570 | PASS |

The 4-query × 5M-group case was stopped by the host for low system memory before producing a round, so it has no after number. Production correctness: all 320 `lib_cache` tests pass (3 Valkey integration tests skipped without a server). Full-size bit-exact checks against the prototype (itself bit-exact to the old code) pass for LONG seq, LONG shuffled and STRING shuffled. A **third single-bit flip** hit the harness shuffle during these checks (row index 67158622 = valid 49758 with bit 26 set, the same bit as the earlier crash), which further supports the hardware-memory diagnosis.

#### Final production code: fused AVG finish + partitioned workers (`CASCADA_MERGE_WORKERS`)

This is the code on the feature branch: the rewritten `TypedAggregationGroups`, AVG emitted straight from group state (`TypedFrameAccumulator.finishWithAverages`), and hash-partitioned workers running on the engine's executor (virtual threads in `CascadaEngineFactory`). Workers = `CASCADA_MERGE_WORKERS`; the default is half the hardware threads, between 1 and 4 (4 on this machine). Output is bit-identical to the old code for every worker count (full-size checks at 1, 4 and 8 workers, LONG and STRING keys).

**Measurement caveat:** during this run the desktop was also running the League of Legends client and the Avast browser (CPU load ~44% before the benchmark started), so absolute times are noisier than the earlier batches. The 1-worker accept phase measured 3.6–4.3 s here versus 3.4 s in the step-1 run; that difference is within this run's round-to-round spread and was not attributed to the code.

| Queries | Groups | Keys | Order | Before (old code) | After, 1 worker | After, 4 workers (default) | After, 8 workers | Merge heap before → after (4 workers) |
|---:|---:|---|---|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 9.28 s | 4.28 s (**2.2×**) | 2.08 s (**4.5×**) | 1.92 s (**4.8×**) | 0.57 → 0.30 GiB (−46%) |
| 1 | 1,000,000 | LONG | shuffled | 22.56 s | 11.96 s (**1.9×**) | 4.26 s (**5.3×**) | 3.46 s (**6.5×**) | 0.58 → 0.30 GiB (−47%) |
| 1 | 1,000,000 | STRING | shuffled | 25.33 s | 16.07 s (**1.6×**) | 5.90 s (**4.3×**) | — | 0.56 → 0.30 GiB (−46%) |
| 1 | 5,000,000 | LONG | shuffled | 31.28 s | 19.06 s (**1.6×**) | 7.41 s (**4.2×**) | 6.59 s (**4.7×**) | 2.61 → 1.64 GiB (−37%) |
| 4 | 1,000,000 | LONG | shuffled | 28.64 s | 16.35 s (**1.8×**) | 10.30 s (**2.8×**) | — | 2.21 → 1.23 GiB (−44%) |

Every round of the final production run:

| Queries | Groups | Keys | Order | Workers | Round | Wall | Accept | Process CPU-s | Avg cores | Merge heap GiB | Verify |
|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 | 1,000,000 | LONG | seq | 1 | 0 | 4.469 s | 4.166 s | 4.6 | 1.03 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 1 | 4.262 s | 4.005 s | 4.4 | 1.03 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 1 | 2 | 4.281 s | 4.043 s | 4.4 | 1.02 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 0 | 2.085 s | 1.805 s | 8.0 | 3.86 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 1 | 2.079 s | 1.820 s | 7.9 | 3.80 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 4 | 2 | 2.059 s | 1.805 s | 7.7 | 3.73 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 0 | 1.915 s | 1.588 s | 12.8 | 6.66 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 1 | 2.102 s | 1.764 s | 13.7 | 6.51 | 0.281 | PASS |
| 1 | 1,000,000 | LONG | seq | 8 | 2 | 1.878 s | 1.587 s | 12.5 | 6.66 | 0.281 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 0 | 12.384 s | 10.990 s | 12.5 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 1 | 11.955 s | 10.818 s | 12.1 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 1 | 2 | 11.838 s | 10.817 s | 11.7 | 0.99 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 0 | 4.261 s | 3.547 s | 15.1 | 3.54 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 1 | 4.283 s | 3.563 s | 15.0 | 3.50 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 4 | 2 | 4.253 s | 3.538 s | 15.0 | 3.54 | 0.305 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 0 | 3.036 s | 2.383 s | 19.4 | 6.38 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 1 | 3.482 s | 2.762 s | 21.7 | 6.24 | 0.289 | PASS |
| 1 | 1,000,000 | LONG | shuffled | 8 | 2 | 3.463 s | 2.790 s | 22.2 | 6.42 | 0.289 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 0 | 16.995 s | 13.768 s | 16.8 | 0.99 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 1 | 15.721 s | 12.515 s | 15.8 | 1.00 | 0.289 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 1 | 2 | 16.068 s | 12.877 s | 16.2 | 1.01 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 0 | 5.896 s | 4.629 s | 20.9 | 3.55 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 1 | 6.089 s | 4.834 s | 22.8 | 3.74 | 0.297 | PASS |
| 1 | 1,000,000 | STRING | shuffled | 4 | 2 | 5.705 s | 4.466 s | 20.8 | 3.65 | 0.297 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 0 | 19.295 s | 11.242 s | 19.5 | 1.01 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 1 | 19.063 s | 11.147 s | 19.3 | 1.01 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 1 | 2 | 17.283 s | 11.229 s | 17.2 | 1.00 | 1.547 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 0 | 7.702 s | 3.413 s | 22.1 | 2.87 | 1.369 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 1 | 7.414 s | 3.549 s | 22.0 | 2.96 | 1.633 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 4 | 2 | 7.333 s | 3.656 s | 21.8 | 2.97 | 1.641 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 0 | 6.589 s | 2.673 s | 30.2 | 4.58 | 1.680 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 1 | 6.702 s | 2.805 s | 31.6 | 4.72 | 1.680 | PASS |
| 1 | 5,000,000 | LONG | shuffled | 8 | 2 | 6.301 s | 2.598 s | 28.5 | 4.53 | 1.680 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 0 | 16.472 s | 14.576 s | 65.1 | 3.95 | 1.219 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 1 | 1 | 16.220 s | 14.430 s | 63.5 | 3.91 | 1.211 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 4 | 0 | 10.318 s | 9.476 s | 121.3 | 11.76 | 1.234 | PASS |
| 4 | 1,000,000 | LONG | shuffled | 4 | 1 | 10.283 s | 9.212 s | 120.8 | 11.75 | 1.227 | PASS |

The 4-query × 5M-group case was not rerun: it exhausted host memory in the previous batch.

#### Correctness

- Bit-exact cross-check (every cell, including row order and double bits) of after vs before: 2M/100k LONG seq w1, 2M/100k LONG shuffled w4, 2M/100k STRING shuffled w3, 3M/1M LONG shuffled w8, 20M/1M LONG seq w1, then a stress run of **80 full 20M-row/1M-group merges** (10 JVMs × 4 repeats × w1 and w8): **all bit-exact** (`raw/stress.log`).
- Three anomalies occurred during the session and are excluded from timing: (1) one wrong AVG (sum off by exactly 2^-17), (2) one worker exception that hung a w8 run (prototype then fixed to fail fast instead of hang), (3) one `ArrayIndexOutOfBoundsException` in the **test-harness shuffle**, before any merge code ran, where index `0x40000E7` is the valid value 231 with bit 26 flipped. Both diagnosable values are single-bit flips, including one outside the merge code, which points to **hardware memory errors on this non-ECC desktop**, not to the merge logic. Recommend MemTest86 / disabling XMP before trusting long benchmark runs on this machine.

#### Why it is faster and smaller

1. **~40% of the old merge time was `SparkValues.compareStrings("double","double")`**: the `MIN(typeof)` AVG metadata column walked code points 60M times per query. After: one dictionary id per value; strings are compared only if ids differ.
2. **Old state layout = ~26 parallel arrays per group** (keys, presence booleans, 9 measure arrays, hashes). Each input row touched ~26 random cache lines; with shuffled input this made the old merge 2.4× slower than the published report's sequential case (22.6 s vs 9.3 s). After: one contiguous `long[]` slab per group (≈1–2 cache lines) + a slot that stores the hash tag, so a mismatch is rejected without touching group memory.
3. **No doubling copies**: group state lives in fixed 16,384-group pages, so growth never copies state or over-allocates up to 2× (the old arrays hit 8.4M capacity for 5M groups).
4. **Column-at-a-time processing**: one hash pass, one probe pass, one tight loop per measure column; no per-cell `switch` on type.
5. **Fused finish**: AVG is computed straight from group state into the 6 output columns, instead of building a 12-column merged frame and then a 6-column AVG frame.
6. **Partitioned parallelism**: each worker owns a disjoint hash partition (no locks, no shared mutable state); results are k-way merged in key order. One query can now use several cores.

#### Limits

One desktop, other applications open, 2–3 measured rounds per case, no p95/p99. In-memory merge only (no cache fetch, decoding, Spark, network). The prototype supports LONG/DOUBLE/STRING keys and DOUBLE AVG; DECIMAL and time-dimension flooring are handled only by the production port. With 4 concurrent queries the machine's 8 physical cores are the ceiling, so threads per query help less (3.0× vs 1.9×).

#### Files

`prototype/` holds the measured prototype and harness; `raw/` holds the raw log lines and `all-runs.json`.

<!-- END CONSOLIDATED SOURCE: docs/benchmarks/merge-redesign-2026-10-18/REPORT.md -->

## Consolidation provenance

Original document bytes were backed up locally before consolidation. All source sections above retain the original prose, tables, commands, and findings; headings and relative links were adapted to the single README. These are historical records, and their test verdicts apply to the snapshots they name.

| Original document | Lines | Original SHA-256 |
|---|---:|---|
| `E2E_README.md` | 347 | `aa2955152fb1a4d474ce2aa3fc778e4573634d30b47ba134f28b681c787b19a9` |
| `docs/design.md` | 90 | `9de9250b8bc38c9ee2db9d999c2cc7d033d69ec1376e555c2ecd4a50945eb581` |
| `docs/CACHE_CORE_FIX_REVIEW_2026-10-16.md` | 83 | `0859832dbb91de8f517c5433c722a7c55ba5038209171e023cbb6dd92a39ae64` |
| `docs/REVIEW_REMEDIATION.md` | 90 | `4202dcf28ce0218729e0c22b68b3d2f4c80cd5275d38a942ccd8ef35f96dcee6` |
| `docs/cache-python-parity-review-codex.md` | 148 | `afe358a3bd67a57c2d6d6082f3c9bf37adfbc41eb01a2568b2ff26552cf4e57c` |
| `docs/freshness-review-codex.md` | 72 | `b7ca3bd8032054a3c041065fa8687b81d22d1b48bcb3bb302252acb911f3a93f` |
| `docs/readability-performance-review.md` | 114 | `d7ed50ce3bfc264bd26d7ef84f8c6765152dfd0ce685a984315fca1fdb851501` |
| `docs/spark-retrieval-review-codex.md` | 76 | `c9db7fc388e21bd4e146807d2974556d6697572ffc143c0327bdc9ba5dbbddf1` |
| `docs/sql-merge-review-codex.md` | 27 | `14c4fb0cd3370060761a25c9b1ec8937929d995b523f90d982c9dcf8702faa46` |
| `docs/benchmarks/merge-redesign-2026-10-18/REPORT.md` | 198 | `5de85d555e02d1c559b1a098ba924bb56e1be90e8a937e68196ddd2e8d4b042c` |
