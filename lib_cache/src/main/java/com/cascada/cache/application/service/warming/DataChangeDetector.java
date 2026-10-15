package com.cascada.cache.application.service.warming;

import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.ValidationBaselinePort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.time.TimeRange;
import com.cascada.cache.domain.validation.ValidationBaseline;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    private static final String VALUE_SEPARATOR = "\u001f";
    private static final String ROW_SEPARATOR = "\u001e";

    private final ValidationBaselinePort baselineStore;
    private final LogicalSqlTranslatorPort translator;
    private final QueryExecutorPort executor;
    private final String validationSqlTemplate;
    private final Duration baselineTimeToLive;

    public DataChangeDetector(ValidationBaselinePort baselineStore, LogicalSqlTranslatorPort translator,
                              QueryExecutorPort executor, String validationSqlTemplate, Duration baselineTimeToLive) {
        this.baselineStore = Objects.requireNonNull(baselineStore, "baselineStore");
        this.translator = Objects.requireNonNull(translator, "translator");
        this.executor = Objects.requireNonNull(executor, "executor");
        this.validationSqlTemplate = Optional.ofNullable(validationSqlTemplate)
                .filter(template -> template.contains("{start}") && template.contains("{end}"))
                .orElseThrow(() -> new IllegalArgumentException(
                        "validation SQL must contain the {start} and {end} placeholders"));
        this.baselineTimeToLive = Objects.requireNonNull(baselineTimeToLive, "baselineTimeToLive");
    }

    /** True when the cached history must be rewritten; also slides the baseline to {@code currentWindow}. */
    public boolean hasHistoricalDataChanged(TimeRange currentWindow) {
        return baselineStore.loadValidationBaseline()
                .map(baseline -> compareAndSlideBaseline(baseline, currentWindow))
                .orElseGet(() -> startNewEpoch(currentWindow));
    }

    private boolean startNewEpoch(TimeRange currentWindow) {
        Optional<String> signature = signatureOf(currentWindow);
        signature.ifPresent(value -> saveBaseline(currentWindow, value));
        return signature.isPresent();
    }

    private boolean compareAndSlideBaseline(ValidationBaseline baseline, TimeRange currentWindow) {
        Optional<String> storedWindowSignature = signatureOf(baseline.window());
        storedWindowSignature
                .flatMap(signature -> currentWindow.equals(baseline.window())
                        ? Optional.of(signature)
                        : signatureOf(currentWindow))
                .ifPresent(signature -> saveBaseline(currentWindow, signature));
        return storedWindowSignature.filter(signature -> !signature.equals(baseline.signature())).isPresent();
    }

    private void saveBaseline(TimeRange window, String signature) {
        baselineStore.saveValidationBaseline(new ValidationBaseline(window, signature), baselineTimeToLive);
    }

    private Optional<String> signatureOf(TimeRange window) {
        String validationSql = validationSqlTemplate
                .replace("{start}", Long.toString(window.startTimestampSeconds()))
                .replace("{end}", Long.toString(window.endTimestampSeconds()));
        try {
            return Optional.of(signature(executor.execute(translator.translateToPhysicalSql(validationSql))));
        } catch (CancellationException cancelled) {
            throw cancelled;
        } catch (RuntimeException validationFailure) {
            LOGGER.warning("validation query failed for window " + window + "; keeping the current baseline ("
                    + validationFailure.getClass().getSimpleName() + ")");
            return Optional.empty();
        }
    }

    /** Column- and row-order independent, so a re-planned query with the same data yields the same value. */
    private static String signature(ResultFrame frame) {
        List<String> columns = frame.columnNames().stream().sorted().toList();
        String canonicalRows = IntStream.range(0, frame.rowCount())
                .mapToObj(row -> columns.stream()
                        .map(column -> String.valueOf(frame.valueAt(row, frame.columnIndex(column))))
                        .collect(Collectors.joining(VALUE_SEPARATOR)))
                .sorted()
                .collect(Collectors.joining(ROW_SEPARATOR));
        return HexFormat.of().formatHex(md5().digest(
                (String.join(VALUE_SEPARATOR, columns) + ROW_SEPARATOR + canonicalRows)
                        .getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest md5() {
        try {
            return MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("MD5 is a required JDK algorithm but was unavailable", impossible);
        }
    }
}
