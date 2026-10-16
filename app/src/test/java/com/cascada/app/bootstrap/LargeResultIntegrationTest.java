package com.cascada.app.bootstrap;

import com.cascada.app.adapter.in.api.QueryResponseEncoder;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.*;

@EnabledIfSystemProperty(named="cascada.large.result.integration", matches="true")
class LargeResultIntegrationTest {
    @Test void returnsFiftyFiveMillionRowsUnderOperatorControlledSparkConfiguration() throws Exception {
        SparkSession session = LocalSparkFixtures.localSession().master("local[4]").appName("cascada-55-million-result")
                .config("spark.driver.maxResultSize", "0").getOrCreate();
        try (var executor = new SparkDeltaQueryExecutor(session)) {
            session.sparkContext().setLogLevel("ERROR");
            long started = System.nanoTime();
            var frame = executor.execute("SELECT id FROM range(0, 55000000, 1, 16)");
            long collected = System.nanoTime();
            assertThat(frame.rowCount()).isEqualTo(55_000_000);
            assertThat(frame.longAt(0, 0)).isZero();
            assertThat(frame.longAt(54_999_999, 0)).isEqualTo(54_999_999);
            QueryResponseEncoder.json(frame, java.io.OutputStream.nullOutputStream());
            System.out.println("55M_RESULT rows=" + frame.rowCount() + " collect_ms=" + (collected - started) / 1_000_000
                    + " json_encode_ms=" + (System.nanoTime() - collected) / 1_000_000);
        } finally { session.stop(); }
    }
}

