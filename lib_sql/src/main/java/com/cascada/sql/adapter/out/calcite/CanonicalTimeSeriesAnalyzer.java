package com.cascada.sql.adapter.out.calcite;

import com.cascada.cache.domain.time.TimeRange;
import com.cascada.sql.domain.TimeDimensionMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

/** Extracts the one time window and time-series shape supported by cache canonicalization. */
final class CanonicalTimeSeriesAnalyzer {

    CanonicalTimeRangeExtraction extractTimeRangeAndFilters(SqlSelect select, TimeDimensionMap dimensions) {
        List<SqlNode> conjuncts = SqlTrees.conjuncts(select.getWhere());

        Long start = null;
        Long end = null;
        List<String> filters = new ArrayList<>();

        for (SqlNode conjunct : conjuncts) {
            CanonicalTimeBound bound = TimeBoundClassifier.classify(conjunct, dimensions);
            if (bound == null) {
                filters.add(CalciteSql.unparse(conjunct));
                continue;
            }
            if (bound.start() != null) {
                start = start == null ? bound.start() : Math.max(start, bound.start());
            }
            if (bound.end() != null) {
                end = end == null ? bound.end() : Math.min(end, bound.end());
            }
        }

        Optional<TimeRange> timeRange = start != null && end != null && end >= start
                ? Optional.of(new TimeRange(start, end))
                : Optional.empty();
        return new CanonicalTimeRangeExtraction(timeRange, new ArrayList<>(new TreeSet<>(filters)));
    }

    CanonicalTimeSeriesShape detectTimeSeries(SqlSelect select, TimeDimensionMap dimensions) {
        SqlNodeList group = select.getGroup();
        if (group == null) {
            return new CanonicalTimeSeriesShape(false, Optional.empty(), false);
        }
        for (SqlNode expression : group) {
            Optional<Integer> step = floorBucketStep(expression, dimensions);
            if (step.isPresent()) {
                return new CanonicalTimeSeriesShape(true, step, false);
            }
        }
        for (SqlNode expression : group) {
            if (expression instanceof SqlIdentifier identifier && SqlTrees.isTimeColumn(identifier, dimensions)) {
                return new CanonicalTimeSeriesShape(true, Optional.empty(), true);
            }
        }
        return new CanonicalTimeSeriesShape(false, Optional.empty(), false);
    }

    /** The step of a recognised {@code FLOOR(t / N) * N} bucket on a time column. */
    static Optional<Integer> floorBucketStep(SqlNode expression, TimeDimensionMap dimensions) {
        return TimeBuckets.match(expression, dimensions).map(TimeBucket::step);
    }
}
