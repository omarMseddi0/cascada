package com.cascada.spark.adapter.out.spark;

import com.cascada.cache.adapter.out.serialization.ArrowResultFrameSerializer;
import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

/** Real local Spark execution, opt-in because it starts a JVM-local engine and worker threads. */
@EnabledIfSystemProperty(named="cascada.spark.integration", matches="true")
class SparkCacheSerializationIntegrationTest {
    @Test void roundTripsRealSparkRowsForEverySupportedScalarType() {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("cascada-cache-codec-integration")
                .config("spark.ui.enabled", "false").config("spark.driver.host", "127.0.0.1")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.session.timeZone", "UTC").getOrCreate();
        try {
            spark.sparkContext().setLogLevel("ERROR");
            var dataset = spark.sql("""
                SELECT CAST(id % 128 AS TINYINT) AS b, CAST(id % 32768 AS SMALLINT) AS sh,
                    CAST(id AS INT) AS i, id + 9007199254740993L AS l,
                    CAST(id * 0.125 AS FLOAT) AS f, CAST(id * 0.125 AS DOUBLE) AS d,
                    CAST(id * 0.127 AS DECIMAL(30,10)) AS dec,
                    CASE WHEN id % 7 = 0 THEN NULL ELSE
                        concat(decode(unhex('E69DB1E4BAACF09F9880'), 'UTF-8'), CAST(id % 16 AS STRING)) END AS s,
                    id % 2 = 0 AS active, date_add(DATE '2026-10-03', CAST(id % 30 AS INT)) AS day,
                    TIMESTAMP '2026-10-03 10:11:12.123456' AS ts,
                    CAST(TIMESTAMP '2026-10-03 10:11:12.123456' AS TIMESTAMP_NTZ) AS ntz,
                    NULL AS nil
                FROM range(1000000)
                """);
            var frame = new SparkResultFrameMapper(1_000_000).map(dataset.schema(), dataset.toLocalIterator());
            assertThat(frame.rowCount()).isEqualTo(1_000_000);
            assertThat(frame.longAt(0,frame.columnIndex("l"))).isEqualTo(9_007_199_254_740_993L);
            assertThat(frame.isNullAt(0,frame.columnIndex("s"))).isTrue();
            assertThat(frame.isNullAt(999_999,frame.columnIndex("nil"))).isTrue();
            for (CacheValueSerializerPort codec : List.of(new PortableFrameSerializer(),new ArrowResultFrameSerializer())) {
                byte[] blob = codec.serialize(frame);
                var decoded = codec.deserialize(blob);
                assertThat(decoded.columnNames()).isEqualTo(frame.columnNames());
                assertThat(decoded.columnTypes()).isEqualTo(frame.columnTypes());
                for (int column = 0; column < frame.columnNames().size(); column++) {
                    var expected = frame.columnReader(column);
                    var actual = decoded.columnReader(column);
                    for (int row = 0; row < frame.rowCount(); row++) {
                        if (expected.isNullAt(row) != actual.isNullAt(row)) throw new AssertionError("null mismatch");
                        if (expected.isNullAt(row)) continue;
                        boolean same = switch (expected.type()) {
                            case LONG -> expected.longValue(row) == actual.longValue(row);
                            case DOUBLE -> Double.doubleToLongBits(expected.doubleValue(row)) == Double.doubleToLongBits(actual.doubleValue(row));
                            case STRING -> expected.stringValue(row).equals(actual.stringValue(row));
                            case DECIMAL -> expected.decimalValue(row).equals(actual.decimalValue(row));
                        };
                        if (!same) throw new AssertionError("mismatch at row " + row + " column " + column);
                    }
                }
            }
        } finally { spark.stop(); }
    }
}
