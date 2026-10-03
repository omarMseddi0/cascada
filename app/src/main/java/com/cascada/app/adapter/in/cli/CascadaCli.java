package com.cascada.app.adapter.in.cli;

import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.in.FlushCacheUseCase;
import com.cascada.cache.application.port.in.MeasureCacheSizeUseCase;
import com.cascada.cache.application.port.in.WarmCacheUseCase;
import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.cache.domain.frame.ResultFrame;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Command-line adapter that translates arguments into inbound use-case calls. */
public final class CascadaCli {

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final int DEFAULT_WARM_LOOKBACK_DAYS = 7;
    private static final Set<String> COMMAND_NAMES = Set.of("query", "cache-size", "flush", "warm");

    private final ExecuteLogicalQueryUseCase executeLogicalQuery;
    private final MeasureCacheSizeUseCase measureCacheSize;
    private final FlushCacheUseCase flushCache;
    private final WarmCacheUseCase warmCache;

    public CascadaCli(ExecuteLogicalQueryUseCase executeLogicalQuery,
                      MeasureCacheSizeUseCase measureCacheSize,
                      FlushCacheUseCase flushCache,
                      WarmCacheUseCase warmCache) {
        this.executeLogicalQuery = Objects.requireNonNull(executeLogicalQuery, "executeLogicalQuery");
        this.measureCacheSize = Objects.requireNonNull(measureCacheSize, "measureCacheSize");
        this.flushCache = Objects.requireNonNull(flushCache, "flushCache");
        this.warmCache = Objects.requireNonNull(warmCache, "warmCache");
    }

    /** Dispatch one command. Unknown or missing commands print usage rather than throwing. */
    public void run(String[] args) {
        if (!validateCommand(args)) return;
        switch (args[0]) {
            case "query" -> runQuery(args);
            case "cache-size" -> runCacheSize();
            case "flush" -> runFlush(args);
            case "warm" -> runWarm();
        }
    }

    /** Validate CLI input before starting cache or Spark infrastructure. */
    public static boolean validateCommand(String[] arguments) {
        if (arguments.length == 0) {
            printUsage();
            return false;
        }
        if (!COMMAND_NAMES.contains(arguments[0])) {
            System.out.println("unknown command: " + arguments[0]);
            printUsage();
            return false;
        }
        if (arguments[0].equals("query") && arguments.length < 2) {
            System.out.println("usage: query \"<logical SQL>\"");
            return false;
        }
        return true;
    }

    private void runQuery(String[] args) {
        ExecuteCachedQueryUseCase.Result result = executeLogicalQuery.query(args[1]);
        // Report whether this result used the cache execution path.
        System.out.println("served through cache: " + result.servedThroughCache());
        printFrame(result.frame());
    }

    private void runCacheSize() {
        CacheSizeReport report = measureCacheSize.measureCacheSize();
        System.out.println(report.totalMegabytes() + " MB across " + report.bucketCount() + " buckets");
    }

    private void runFlush(String[] args) {
        long purged = args.length > 1 ? flushCache.flushKeyPrefix(args[1]) : flushCache.flushEverything();
        System.out.println("purged " + purged + " buckets");
    }

    private void runWarm() {
        long nowSeconds = System.currentTimeMillis() / 1000L;
        WarmCacheUseCase.Report report = warmCache.warmCycle(
                nowSeconds - DEFAULT_WARM_LOOKBACK_DAYS * SECONDS_PER_DAY, nowSeconds, false);
        System.out.println("patterns=" + report.patternsWarmed()
                + " warmed=" + report.bucketsWarmed() + " skipped=" + report.bucketsSkipped());
    }

    private void printFrame(ResultFrame frame) {
        System.out.println(String.join(" | ", frame.columnNames()));
        for (Map<String, Object> row : frame.rows()) {
            StringBuilder line = new StringBuilder();
            for (String column : frame.columnNames()) {
                if (line.length() > 0) {
                    line.append(" | ");
                }
                line.append(row.get(column));
            }
            System.out.println(line);
        }
        System.out.println("(" + frame.rowCount() + " rows)");
    }

    private static void printUsage() {
        System.out.println("""
                cascada <command>

                  query "<logical SQL>"   run a query through the cache
                  cache-size              report stored bytes and bucket count
                  flush [keyPrefix]       purge everything, or only keys with this prefix
                  warm                    run one warming cycle over the last 7 days
                """);
    }
}
