package com.cascada.cache.application.service.warming;

import com.cascada.cache.application.model.warming.ValidationCheck;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.ValidationBaselinePort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * The data-change canary: decides whether historical data inside the warming window changed since the
 * cached buckets were computed (late arrivals, backfills, Delta MERGE/DELETE). Buckets are immutable once
 * complete only as long as their source rows are, and this is what notices when they are not.
 *
 * <p>Ungrouped validation uses one signature for its historical window.
 * <ul>
 *   <li>no baseline (first run, expiry, unreadable): baseline the current window and report a change, so
 *       the first epoch rewrites every bucket;</li>
 *   <li>baseline present: re-run the validation query over the <em>stored</em> window — a different
 *       signature means already-cached history changed — then re-baseline to the current window, so a clean
 *       slide forward never forces a rewrite;</li>
 *   <li>a failed validation query is conservative: no forced rewrite and the baseline is left untouched,
 *       because a transient failure must never destroy a healthy cache.</li>
 * </ul>
 *
 * <p>The validation SQL is logical SQL with {@code {start}} and {@code {end}} placeholders for the inclusive
 * epoch-second window, for example
 * {@code SELECT SUM(bytes) FROM main_data_table WHERE ts >= {start} AND ts <= {end}}. Any number of rows and
 * columns is allowed in ungrouped mode. Grouped mode requires {@code {bucketSeconds}} in the
 * template and hashes one row per aligned {@code bucket_start}. It compares against the stored
 * bucket signatures before warming and rechecks the same window after warming.
 */
public final class DataChangeDetector {

    private static final String BUCKET_WIDTH = "{bucketSeconds}";
    private static final String BUCKET_START = "bucket_start";

    private static final Logger LOGGER = Logger.getLogger(DataChangeDetector.class.getName());

    private final ValidationBaselinePort baselineStore;
    private final LogicalSqlTranslatorPort translator;
    private final QueryExecutorPort executor;
    private final String validationSqlTemplate;
    private final String timeColumn;
    private final Duration baselineTimeToLive;

    public DataChangeDetector(ValidationBaselinePort baselineStore, LogicalSqlTranslatorPort translator,
                              QueryExecutorPort executor, String validationSqlTemplate, Duration baselineTimeToLive) {
        this(baselineStore, translator, executor, validationSqlTemplate, baselineTimeToLive, "ts");
    }

    public DataChangeDetector(ValidationBaselinePort baselineStore, LogicalSqlTranslatorPort translator,
                              QueryExecutorPort executor, String validationSqlTemplate, Duration baselineTimeToLive,
                              String timeColumn) {
        this.timeColumn = Objects.requireNonNull(timeColumn);
        this.baselineStore = Objects.requireNonNull(baselineStore, "baselineStore");
        this.translator = Objects.requireNonNull(translator, "translator");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.validationSqlTemplate = Optional.ofNullable(validationSqlTemplate)
                .filter(template -> template.contains("{timestamp}") || template.contains("{start}") && template.contains("{end}"))
                .orElseThrow(() -> new IllegalArgumentException(
                        "validation SQL must contain {timestamp}, or {start} and {end}"));
        this.baselineTimeToLive = Objects.requireNonNull(baselineTimeToLive, "baselineTimeToLive");
        if (baselineTimeToLive.isNegative() || baselineTimeToLive.isZero())
            throw new IllegalArgumentException("validation baseline TTL must be positive");
    }

    /** Proposes a baseline; it is saved only by {@link #acknowledge} after the warmer completes its refresh. */
    public ValidationCheck check(TimeRange currentWindow) {
        Optional<ValidationBaseline> stored = baselineStore.loadValidationBaseline();
        if (stored.isEmpty()) {
            Optional<String> value = signatureOf(currentWindow);
            return new ValidationCheck(value.isPresent(), value.map(signature -> new ValidationBaseline(currentWindow, signature)));
        }
        ValidationBaseline baseline = stored.get();
        Optional<String> previous = signatureOf(baseline.window());
        if (previous.isEmpty()) return new ValidationCheck(false, Optional.empty());
        boolean changed = !previous.get().equals(baseline.signature());
        Optional<String> current = currentWindow.equals(baseline.window()) ? previous : signatureOf(currentWindow);
        return new ValidationCheck(changed, current.map(signature -> new ValidationBaseline(currentWindow, signature)));
    }

    /** Grouped SQL opts in with {bucketSeconds} and must return one BIGINT bucket_start per row. */
    public boolean validatesBuckets() {
        return validationSqlTemplate.contains(BUCKET_WIDTH);
    }

    public ValidationCheck checkBuckets(TimeRange window, long bucketSeconds) {
        if (bucketSeconds <= 0 || Math.floorMod(window.startTimestampSeconds(), bucketSeconds) != 0
                || Math.floorMod(window.endTimestampSeconds(), bucketSeconds) != bucketSeconds - 1) {
            throw new IllegalArgumentException("Grouped validation requires a window aligned to complete cache buckets");
        }
        Optional<Map<Long, String>> current = bucketSignatures(window, bucketSeconds);
        if (current.isEmpty()) return new ValidationCheck(false, Optional.empty());

        Map<Long, String> signatures = current.orElseThrow();
        String prefix = bucketSignaturePrefix(bucketSeconds);
        String encoded = signatures.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue())
                .collect(Collectors.joining(",", prefix, ""));
        Optional<ValidationBaseline> next = Optional.of(new ValidationBaseline(window, encoded));
        Optional<ValidationBaseline> baseline = baselineStore.loadValidationBaseline();
        if (baseline.isEmpty() || !baseline.orElseThrow().signature().startsWith(prefix)) {
            return new ValidationCheck(true, next);
        }

        ValidationBaseline previous = baseline.orElseThrow();
        Map<Long, String> old;
        try {
            old = decodeBucketSignatures(previous.signature(), prefix);
        } catch (IllegalArgumentException unreadable) {
            return new ValidationCheck(true, next);
        }
        Set<Long> changed = changedBuckets(old, signatures);
        // A bucket outside the current lookback is no longer eligible for refresh. A bucket
        // entering it has no previous signature, even if the validation SQL returned no rows.
        changed.removeIf(bucket -> !contains(window, bucket));
        for (long bucket = window.startTimestampSeconds(); bucket <= window.endTimestampSeconds();) {
            if (!contains(previous.window(), bucket)) changed.add(bucket);
            if (bucket > Long.MAX_VALUE - bucketSeconds) break;
            bucket += bucketSeconds;
        }
        return new ValidationCheck(!changed.isEmpty(), next, changed);
    }

    /** Checks the same window again before accepting the proposed baseline. */
    public Optional<Set<Long>> changesDuringWarming(ValidationBaseline proposed, long bucketSeconds) {
        Map<Long, String> before = decodeBucketSignatures(proposed.signature(), bucketSignaturePrefix(bucketSeconds));
        return bucketSignatures(proposed.window(), bucketSeconds).map(after -> changedBuckets(before, after));
    }

    private static String bucketSignaturePrefix(long bucketSeconds) {
        return "b1/" + bucketSeconds + "/";
    }

    private static Map<Long, String> decodeBucketSignatures(String encoded, String prefix) {
        String values = encoded.substring(prefix.length());
        Map<Long, String> signatures = new HashMap<>();
        if (!values.isEmpty()) {
            for (String entry : values.split(",")) {
                int separator = entry.indexOf('=');
                if (separator < 1 || entry.length() - separator - 1 != 64) {
                    throw new IllegalArgumentException("Unreadable bucket signature");
                }
                signatures.put(Long.parseLong(entry.substring(0, separator)), entry.substring(separator + 1));
            }
        }
        return signatures;
    }

    private static Set<Long> changedBuckets(Map<Long, String> before, Map<Long, String> after) {
        Set<Long> changed = new HashSet<>(before.keySet());
        changed.addAll(after.keySet());
        changed.removeIf(bucket -> Objects.equals(before.get(bucket), after.get(bucket)));
        return changed;
    }

    private static boolean contains(TimeRange range, long timestamp) {
        return timestamp >= range.startTimestampSeconds() && timestamp <= range.endTimestampSeconds();
    }

    private Optional<Map<Long, String>> bucketSignatures(TimeRange window, long bucketSeconds) {
        try {
            ResultFrame frame = executeValidation(window, validationSqlTemplate.replace(BUCKET_WIDTH, Long.toString(bucketSeconds)));
            if (!frame.columnNames().contains(BUCKET_START) || frame.columnType(BUCKET_START) != ColumnType.LONG) {
                throw new IllegalArgumentException("Grouped validation must return a BIGINT bucket_start column");
            }
            List<String> columns = frame.columnNames().stream().sorted().toList();
            int bucketIndex = frame.columnIndex(BUCKET_START);
            Map<Long, String> signatures = new TreeMap<>();
            for (int row = 0; row < frame.rowCount(); row++) {
                if (frame.isNullAt(row, bucketIndex)) throw new IllegalArgumentException("Null bucket_start");
                long bucket = frame.longAt(row, bucketIndex);
                if (!contains(window, bucket) || Math.floorMod(bucket, bucketSeconds) != 0) {
                    throw new IllegalArgumentException("Unaligned or out-of-window bucket_start: " + bucket);
                }
                String signature = HexFormat.of().formatHex(rowSignature(frame, columns, row));
                if (signatures.putIfAbsent(bucket, signature) != null) {
                    throw new IllegalArgumentException("Multiple validation rows for bucket_start " + bucket);
                }
            }
            return Optional.of(signatures);
        } catch (RuntimeException failure) {
            QueryCancellation.rethrowIfCancelled(failure);
            LOGGER.warning("Grouped validation failed for " + window + ": " + failure.getMessage());
            return Optional.empty();
        }
    }

    public void acknowledge(ValidationCheck check) {
        check.nextBaseline().ifPresent(baseline -> baselineStore.saveValidationBaseline(baseline, baselineTimeToLive));
    }

    /** Convenience for standalone checks; scheduled warming uses check/acknowledge around its writes. */
    public boolean hasHistoricalDataChanged(TimeRange currentWindow) {
        ValidationCheck check = check(currentWindow);
        acknowledge(check);
        return check.changed();
    }

    private Optional<String> signatureOf(TimeRange window) {
        try {
            return Optional.of(signature(executeValidation(window, validationSqlTemplate)));
        } catch (RuntimeException validationFailure) {
            QueryCancellation.rethrowIfCancelled(validationFailure);
            LOGGER.warning("validation query failed for window " + window + "; keeping the current baseline ("
                    + validationFailure.getClass().getSimpleName() + ")");
            return Optional.empty();
        }
    }

    private ResultFrame executeValidation(TimeRange window, String template) {
        String validationSql = template
                .replace("{timestamp}", "`" + timeColumn.replace("`", "``") + "` >= " + window.startTimestampSeconds()
                        + " AND `" + timeColumn.replace("`", "``") + "` <= " + window.endTimestampSeconds())
                .replace("{start}", Long.toString(window.startTimestampSeconds()))
                .replace("{end}", Long.toString(window.endTimestampSeconds()));
        return executor.execute(translator.translateToPhysicalSql(validationSql));
    }

    /** Column- and row-order independent, so a re-planned query with the same data yields the same value. */
    private static String signature(ResultFrame frame) {
        List<String> columns = frame.columnNames().stream().sorted().toList();
        MessageDigest digest = digest();
        for (String column : columns) {
            update(digest, column);
            update(digest, frame.columnType(column).name());
        }
        // Fixed-size row digests avoid retaining a second copy of every cell's text while sorting.
        List<byte[]> rows = new java.util.ArrayList<>(frame.rowCount());
        for (int row = 0; row < frame.rowCount(); row++) {
            rows.add(rowSignature(frame, columns, row));
        }
        rows.sort(java.util.Arrays::compareUnsigned);
        rows.forEach(digest::update);
        return HexFormat.of().formatHex(digest.digest());
    }

    private static byte[] rowSignature(ResultFrame frame, List<String> columns, int row) {
        MessageDigest digest = digest();
        for (String column : columns) {
            int index = frame.columnIndex(column);
            update(digest, frame.columnTypeAt(index).name());
            digest.update((byte) (frame.isNullAt(row, index) ? 0 : 1));
            if (!frame.isNullAt(row, index)) update(digest, String.valueOf(frame.valueAt(row, index)));
        }
        return digest.digest();
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is a required JDK algorithm but was unavailable", impossible);
        }
    }
}
