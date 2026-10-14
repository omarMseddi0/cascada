package com.cascada.spark.adapter.out.spark;

import java.util.Iterator;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import scala.Option;
import scala.Tuple2;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Computes every result partition in one Spark job and decodes rows lazily on the driver.
 *
 * <p>{@code Dataset.toLocalIterator} submits one job per result partition, one after another, so a
 * scan without a shuffle runs on a single core and every sorted partition pays job-scheduling latency.
 * Here the partitions run in parallel and travel as Spark's compressed row bytes; each row is decoded
 * only as the frame mapper consumes it. The serialized result must fit Spark's own
 * {@code spark.driver.maxResultSize}; when it does not, the caller streams with {@code toLocalIterator},
 * so that operator setting never caps the rows a query can return.
 */
final class ParallelResultCollector {

    private static final Logger LOGGER = Logger.getLogger(ParallelResultCollector.class.getName());

    /** Empty when the dataset is not a classic Spark dataset or its result exceeds maxResultSize. */
    Optional<Iterator<Row>> collect(Dataset<Row> dataset) {
        if (!(dataset instanceof org.apache.spark.sql.classic.Dataset<Row> classic)) {
            return Optional.empty();
        }
        QueryExecution execution = classic.queryExecution();
        ExpressionEncoder<Row> encoder = classic.exprEnc();
        ExpressionEncoder.Deserializer<Row> deserializer = encoder
                .resolveAndBind(execution.analyzed().output(), encoder.resolveAndBind$default$2())
                .createDeserializer();
        Tuple2<Object, scala.collection.Iterator<InternalRow>> collected;
        try {
            collected = SQLExecution.withNewExecutionId(execution, Option.apply("collect"), () -> {
                execution.executedPlan().resetMetrics();
                return execution.executedPlan().executeCollectIterator();
            });
        } catch (Exception failure) {
            // Scala code throws SparkException without declaring it; rethrow every other failure unchanged.
            if (!exceedsMaxResultSize(failure)) {
                throw ParallelResultCollector.<RuntimeException>rethrow(failure);
            }
            LOGGER.log(Level.INFO, "Result exceeds spark.driver.maxResultSize; streaming partitions instead", failure);
            return Optional.empty();
        }
        Iterator<InternalRow> rows = CollectionConverters.asJava(collected._2());
        return Optional.of(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rows.hasNext();
            }

            @Override
            public Row next() {
                return deserializer.apply(rows.next());
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static <T extends Exception> T rethrow(Exception failure) throws T {
        throw (T) failure;
    }

    private static boolean exceedsMaxResultSize(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            String message = cause.getMessage();
            if (message != null && message.contains("spark.driver.maxResultSize")) {
                return true;
            }
        }
        return false;
    }
}
