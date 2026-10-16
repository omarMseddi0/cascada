package com.cascada.app.bootstrap;

import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * The in-process Spark setup shared by the app's Spark tests and benchmarks: no UI and a driver bound
 * to loopback, so a test session never opens a port on the network or races another test for 4040.
 */
final class LocalSparkFixtures {

    static final Map<String, String> LOOPBACK = Map.of(
            "spark.ui.enabled", "false",
            "spark.driver.host", "127.0.0.1",
            "spark.driver.bindAddress", "127.0.0.1");

    private static final ObjectMapper JSON = new ObjectMapper();

    private LocalSparkFixtures() {
    }

    /** A plain {@link SparkSession} builder with the loopback settings. */
    static SparkSession.Builder localSession() {
        SparkSession.Builder builder = SparkSession.builder();
        LOOPBACK.forEach(builder::config);
        return builder;
    }

    /** {@link #localSession()} with the Delta extension and catalog the product sessions use. */
    static SparkSession.Builder localDeltaSession() {
        return localSession()
                .config("spark.sql.extensions", SparkSessionConfigBuilder.DELTA_EXTENSIONS)
                .config("spark.sql.catalog.spark_catalog", SparkSessionConfigBuilder.DELTA_CATALOG);
    }

    /** The product session builder ({@link SparkSessionConfigBuilder#forLocal()}) with the loopback settings. */
    static SparkSessionConfigBuilder localConfig() {
        SparkSessionConfigBuilder builder = SparkSessionConfigBuilder.forLocal();
        LOOPBACK.forEach(builder::withProperty);
        return builder;
    }

    /** Order-sensitive SHA-256 of the rows' JSON cells: equal digests mean identical results. */
    static String digest(List<Row> rows) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (Row row : rows) {
            List<Object> cells = new ArrayList<>(row.size());
            for (int column = 0; column < row.size(); column++) cells.add(row.get(column));
            digest.update(JSON.writeValueAsBytes(cells));
            digest.update((byte) '\n');
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
