package com.cascada.cache.application.service.warming;

import com.cascada.cache.application.model.warming.ValidationCheck;
import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.ValidationBaselinePort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.time.TimeRange;
import com.cascada.cache.domain.model.validation.ValidationBaseline;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * The data-change canary: decides whether historical data inside the warming window changed since the
 * cached buckets were computed (late arrivals, backfills, Delta MERGE/DELETE). Buckets are immutable once
 * complete only as long as their source rows are, and this is what notices when they are not.
 *
 * <p>Sliding-window model with one fixed baseline key, checked once per warming cycle:
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
 * columns is allowed; the result is reduced to one order-independent signature.
 */
public final class DataChangeDetector {

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
        String validationSql = validationSqlTemplate
                .replace("{timestamp}", "`" + timeColumn.replace("`", "``") + "` >= " + window.startTimestampSeconds()
                        + " AND `" + timeColumn.replace("`", "``") + "` <= " + window.endTimestampSeconds())
                .replace("{start}", Long.toString(window.startTimestampSeconds()))
                .replace("{end}", Long.toString(window.endTimestampSeconds()));
        try {
            return Optional.of(signature(executor.execute(translator.translateToPhysicalSql(validationSql))));
        } catch (RuntimeException validationFailure) {
            QueryCancellation.rethrowIfCancelled(validationFailure);
            LOGGER.warning("validation query failed for window " + window + "; keeping the current baseline ("
                    + validationFailure.getClass().getSimpleName() + ")");
            return Optional.empty();
        }
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
            MessageDigest rowDigest = digest();
            for (String column : columns) {
                int index = frame.columnIndex(column);
                update(rowDigest, frame.columnTypeAt(index).name());
                rowDigest.update((byte) (frame.isNullAt(row, index) ? 0 : 1));
                if (!frame.isNullAt(row, index)) update(rowDigest, String.valueOf(frame.valueAt(row, index)));
            }
            rows.add(rowDigest.digest());
        }
        rows.sort(java.util.Arrays::compareUnsigned);
        rows.forEach(digest::update);
        return HexFormat.of().formatHex(digest.digest());
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
