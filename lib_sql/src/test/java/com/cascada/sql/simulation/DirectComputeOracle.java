package com.cascada.sql.simulation;

import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.port.SparkQueryExecutorPort;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The independent ground-truth engine for the cache-correctness gate â€” the Java heir to the Tablesaw
 * "direct compute" oracle in {@code in_memory_simulation.py}. It aggregates fake events in-process and
 * also serves as the fake {@link SparkQueryExecutorPort}: when the cache engine hands it a (gap or
 * full) SQL string, it extracts the {@code ts} ranges from that SQL and aggregates only those events,
 * exactly as a real Spark gap query would.
 *
 * <p>Cache buckets are computed at the fixed internal step (300s); the ground truth is computed
 * independently at the user's step, so a match proves the engine's merge does the same math.
 */
final class DirectComputeOracle implements SparkQueryExecutorPort {

    static final int FIXED_STEP_SECONDS = 300;
    private static final Pattern TS_RANGE =
            Pattern.compile("ts\\s*>=\\s*(\\d+)\\s+AND\\s+ts\\s*<=\\s*(\\d+)");

    record TrafficEvent(long timestampSeconds, String appName, String deviceType, double bytes, double latency) {
    }

    /** A measure to compute: the SQL function, the source field, and the output column name. */
    record MeasureSpec(String function, String field, String outputColumn) {
    }

    private final List<TrafficEvent> events;
    private final List<String> dimensions;
    private final List<MeasureSpec> measures;
    private final boolean isTimeSeries;

    DirectComputeOracle(List<TrafficEvent> events, List<String> dimensions, List<MeasureSpec> measures,
                        boolean isTimeSeries) {
        this.events = List.copyOf(events);
        this.dimensions = List.copyOf(dimensions);
        this.measures = List.copyOf(measures);
        this.isTimeSeries = isTimeSeries;
    }

    /** Fake Spark: aggregate only the events inside the SQL's ts ranges, at the fixed step. */
    @Override
    public ResultFrame execute(String physicalSql) {
        List<long[]> ranges = extractRanges(physicalSql);
        List<TrafficEvent> selected = filterByRanges(ranges);
        return aggregate(selected, FIXED_STEP_SECONDS);
    }

    /** Compute one whole day's bucket frame at the fixed step (used to prime the cache backend). */
    ResultFrame computeDayBucket(long dayStartSeconds, long bucketSeconds) {
        List<long[]> ranges = List.of(new long[]{dayStartSeconds, dayStartSeconds + bucketSeconds - 1});
        return aggregate(filterByRanges(ranges), FIXED_STEP_SECONDS);
    }

    /** The independent ground truth: aggregate the full range directly at the user's step. */
    ResultFrame computeGroundTruth(long startSeconds, long endSeconds, int userStepSeconds) {
        List<TrafficEvent> selected = filterByRanges(List.of(new long[]{startSeconds, endSeconds}));
        return aggregate(selected, userStepSeconds);
    }

    private List<long[]> extractRanges(String sql) {
        List<long[]> ranges = new ArrayList<>();
        Matcher matcher = TS_RANGE.matcher(sql);
        while (matcher.find()) {
            ranges.add(new long[]{Long.parseLong(matcher.group(1)), Long.parseLong(matcher.group(2))});
        }
        return ranges;
    }

    private List<TrafficEvent> filterByRanges(List<long[]> ranges) {
        List<TrafficEvent> selected = new ArrayList<>();
        for (TrafficEvent event : events) {
            for (long[] range : ranges) {
                if (event.timestampSeconds() >= range[0] && event.timestampSeconds() <= range[1]) {
                    selected.add(event);
                    break;
                }
            }
        }
        return selected;
    }

    private ResultFrame aggregate(List<TrafficEvent> selected, int stepSeconds) {
        Map<String, Map<String, Object>> groups = new TreeMap<>();
        for (TrafficEvent event : selected) {
            Long bucketStart = isTimeSeries
                    ? Math.floorDiv(event.timestampSeconds(), stepSeconds) * (long) stepSeconds
                    : null;
            String key = (bucketStart == null ? "" : bucketStart + "|") + dimensionKey(event);
            Map<String, Object> group = groups.computeIfAbsent(key, ignored -> newGroup(event, bucketStart));
            for (MeasureSpec measure : measures) {
                accumulate(group, measure, event);
            }
        }

        ResultFrame.Builder builder = ResultFrame.builder();
        if (isTimeSeries) {
            builder.column("ts", ColumnType.LONG);
        }
        dimensions.forEach(dimension -> builder.column(dimension, ColumnType.STRING));
        measures.forEach(measure -> builder.column(measure.outputColumn(), ColumnType.DOUBLE));
        for (Map<String, Object> group : groups.values()) {
            builder.row(group);
        }
        return builder.build();
    }

    private Map<String, Object> newGroup(TrafficEvent event, Long bucketStart) {
        Map<String, Object> group = new LinkedHashMap<>();
        if (isTimeSeries) {
            group.put("ts", bucketStart);
        }
        for (String dimension : dimensions) {
            group.put(dimension, dimensionValue(event, dimension));
        }
        for (MeasureSpec measure : measures) {
            group.put(measure.outputColumn(), 0.0);
        }
        return group;
    }

    private void accumulate(Map<String, Object> group, MeasureSpec measure, TrafficEvent event) {
        double current = ((Number) group.get(measure.outputColumn())).doubleValue();
        double contribution = switch (measure.function()) {
            case "SUM" -> fieldValue(event, measure.field());
            case "COUNT" -> 1.0;
            default -> throw new IllegalArgumentException("oracle does not support function " + measure.function());
        };
        group.put(measure.outputColumn(), current + contribution);
    }

    private String dimensionKey(TrafficEvent event) {
        StringBuilder key = new StringBuilder();
        for (String dimension : dimensions) {
            key.append(dimensionValue(event, dimension)).append('|');
        }
        return key.toString();
    }

    private String dimensionValue(TrafficEvent event, String dimension) {
        return switch (dimension) {
            case "appName" -> event.appName();
            case "deviceType" -> event.deviceType();
            default -> throw new IllegalArgumentException("unknown dimension " + dimension);
        };
    }

    private double fieldValue(TrafficEvent event, String field) {
        return switch (field) {
            case "bytes" -> event.bytes();
            case "latency" -> event.latency();
            default -> throw new IllegalArgumentException("unknown field " + field);
        };
    }
}
