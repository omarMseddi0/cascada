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

/**
 * <b>Primary (driving) adapter</b> — a command-line front end over the inbound ports.
 *
 * <p>It is the smallest possible demonstration of the hexagon's central promise: this class holds four
 * interfaces and translates {@code String[] args} into calls on them. It knows nothing about Valkey,
 * Spark, Calcite, bucket arithmetic, or safety rules. A REST controller would be the same shape with
 * different plumbing, which is exactly why swapping one for the other changes no application code.
 *
 * <p>Being an adapter, it is also the correct place for presentation concerns — parsing arguments,
 * formatting a frame as text, choosing an exit code. None of that belongs any further in.
 *
 * <h2>Deliberate limitations</h2>
 * This is a thin operational tool, not a product surface:
 * <ul>
 *   <li>TODO(cascada): argument parsing is positional and unvalidated beyond arity. A real CLI wants a
 *       parser (picocli or equivalent), {@code --help}, and typed options.</li>
 *   <li>TODO(cascada): results print as plain text. Add {@code --format=csv|json} for scripting.</li>
 *   <li>TODO(cascada): {@code warm} uses a hard-coded 7-day lookback ending now. It should accept an
 *       explicit window, and {@code now} should come from an injected clock rather than
 *       {@link System#currentTimeMillis()} — an untestable time source at the edge is tolerable, one in
 *       the core would not be.</li>
 *   <li>TODO(cascada): {@code flush} performs no confirmation and takes no tenant, so only the
 *       flush-everything and flush-by-prefix scopes are reachable from here.</li>
 * </ul>
 */
public final class CascadaCli {

    private static final long SECONDS_PER_DAY = 86_400L;
    private static final int DEFAULT_WARM_LOOKBACK_DAYS = 7;

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
        if (args.length == 0) {
            printUsage();
            return;
        }
        switch (args[0]) {
            case "query" -> runQuery(args);
            case "cache-size" -> runCacheSize();
            case "flush" -> runFlush(args);
            case "warm" -> runWarm();
            default -> {
                System.out.println("unknown command: " + args[0]);
                printUsage();
            }
        }
    }

    private void runQuery(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: query \"<logical SQL>\"");
            return;
        }
        ExecuteCachedQueryUseCase.Result result = executeLogicalQuery.query(args[1]);
        // Whether the cache served it is the single most useful diagnostic: false means a safety rule
        // forced a bypass, so the answer is correct but was paid for in full.
        System.out.println("served through cache: " + result.servedThroughCache());
        printFrame(result.frame());
    }

    private void runCacheSize() {
        CacheSizeReport report = measureCacheSize.measureCacheSize();
        System.out.println(report.totalMegabytes() + " MB across " + report.bucketCount() + " buckets");
        report.bytesByTenant().forEach((tenant, bytes) -> System.out.println("  " + tenant + ": " + bytes + " B"));
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

    private void printUsage() {
        System.out.println("""
                cascada <command>

                  query "<logical SQL>"   run a query through the cache
                  cache-size              report stored bytes and bucket count
                  flush [keyPrefix]       purge everything, or only keys with this prefix
                  warm                    run one warming cycle over the last 7 days
                """);
    }
}
