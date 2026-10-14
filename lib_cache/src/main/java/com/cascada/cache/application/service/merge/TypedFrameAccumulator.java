package com.cascada.cache.application.service.merge;

import com.cascada.cache.application.port.out.DistinctSketchPort;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.FrameColumnReader;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.frame.ResultFrameBuilder;
import com.cascada.cache.domain.merge.AggregateFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.cascada.cache.application.service.merge.TypedFrameAggregator.*;

public final class TypedFrameAccumulator {
    private final ResultFrame schema;
    private final List<String> dimensions;
    private final List<String> measures;
    private final int[] dimensionColumns;
    private final ColumnType[] dimensionTypes;
    private final boolean[] timeDimensions;
    private final int step;
    private final TypedAggregationGroups groups;

    public TypedFrameAccumulator(ResultFrame inputSchema, List<String> dimensions,
                       Map<String, AggregateFunction> functions, String timeColumn, int step) {
        this(inputSchema, dimensions, functions, timeColumn, step, null);
    }

    /** {@code sketches} merges APPROX_COUNT_DISTINCT measures; it may be null when none are present. */
    public TypedFrameAccumulator(ResultFrame inputSchema, List<String> dimensions,
                       Map<String, AggregateFunction> functions, String timeColumn, int step,
                       DistinctSketchPort sketches) {
        ResultFrameBuilder descriptor = ResultFrame.builder();
        for (String column : inputSchema.columnNames()) descriptor.column(column, inputSchema.columnType(column));
        this.schema = descriptor.build();
        this.dimensions = List.copyOf(dimensions);
        this.step = step;
        measures = selectMeasures(schema, dimensions, functions);

        dimensionColumns = new int[dimensions.size()];
        dimensionTypes = new ColumnType[dimensions.size()];
        timeDimensions = new boolean[dimensions.size()];
        for (int dimension = 0; dimension < dimensions.size(); dimension++) {
            String name = dimensions.get(dimension);
            dimensionColumns[dimension] = schema.columnNames().indexOf(name);
            if (dimensionColumns[dimension] >= 0) {
                dimensionTypes[dimension] = schema.columnType(name);
            }
            timeDimensions[dimension] = name.equals(timeColumn);
        }

        int[] measureColumns = new int[measures.size()];
        ColumnType[] measureTypes = new ColumnType[measures.size()];
        AggregateOperation[] measureFunctions = new AggregateOperation[measures.size()];
        boolean[] sketchMeasures = new boolean[measures.size()];
        for (int measure = 0; measure < measures.size(); measure++) {
            String name = measures.get(measure);
            measureColumns[measure] = schema.columnIndex(name);
            measureTypes[measure] = schema.columnType(name);
            // Name normalization and aggregate resolution belong outside the row/measure hot loop.
            AggregateFunction function = AggregateFunctionResolver.resolve(name, functions);
            if (function == AggregateFunction.DISTINCT_SKETCH) {
                if (measureTypes[measure] != ColumnType.STRING) {
                    throw new IllegalArgumentException("distinct sketch column '" + name + "' must be an encoded STRING");
                }
                if (sketches == null) {
                    throw new IllegalStateException("no distinct sketch merger is configured for column '" + name + "'");
                }
                sketchMeasures[measure] = true;
            } else {
                measureFunctions[measure] = AggregateOperation.fromFunction(function);
            }
        }

        groups = new TypedAggregationGroups(schema, dimensionColumns, dimensionTypes, timeDimensions, step,
                measureColumns, measureTypes, measureFunctions, sketchMeasures, sketches);
    }

    public void accept(ResultFrame input) {
        validateCompatibleSchemas(List.of(schema, input));
        if (input.rowCount() == 0) {
            return;
        }
        FrameColumnReader[] columns = new FrameColumnReader[input.columnNames().size()];
        for (int column = 0; column < columns.length; column++) columns[column] = input.columnReader(column);

        if (dimensions.isEmpty()) {
            int globalGroup = groups.findOrCreate(columns, 0);
            for (int row = 0; row < input.rowCount(); row++) {
                groups.accumulate(globalGroup, columns, row);
            }
            return;
        }

        for (int row = 0; row < input.rowCount(); row++) {
            int group = groups.findOrCreate(columns, row);
            groups.accumulate(group, columns, row);
        }
    }

    public ResultFrame finish() {
        ResultFrameBuilder result = ResultFrame.builder().expectedRows(groups.groupCount);
        int[] outputDimensions = new int[schema.columnNames().size()];
        int[] outputMeasures = new int[schema.columnNames().size()];
        Arrays.fill(outputDimensions, -1);
        Arrays.fill(outputMeasures, -1);
        for (int column = 0; column < schema.columnNames().size(); column++) {
            String name = schema.columnNames().get(column);
            ColumnType outputType = schema.columnType(name);
            for (int dimension = 0; dimension < dimensions.size(); dimension++) {
                if (dimensions.get(dimension).equals(name)) {
                    outputDimensions[column] = dimension;
                    break;
                }
            }
            if (outputDimensions[column] < 0) {
                for (int measure = 0; measure < measures.size(); measure++) {
                    if (measures.get(measure).equals(name)) {
                        outputMeasures[column] = measure;
                        outputType = groups.outputType(measure);
                        break;
                    }
                }
            }
            result.column(name, outputType);
        }

        int[] order = groups.sortedGroupIds(dimensions, dimensionColumns, dimensionTypes, timeDimensions, step);
        for (int group : order) {
            for (int column = 0; column < schema.columnNames().size(); column++) {
                int dimension = outputDimensions[column];
                if (dimension >= 0) {
                    groups.appendDimension(result, group, dimension, dimensionTypes[dimension],
                    timeDimensions[dimension]);
                } else {
                    groups.appendMeasure(result, group, outputMeasures[column]);
                }
            }
        }
        return result.build();
    }

}
