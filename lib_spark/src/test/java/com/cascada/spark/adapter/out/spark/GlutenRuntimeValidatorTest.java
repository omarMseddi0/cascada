package com.cascada.spark.adapter.out.spark;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class GlutenRuntimeValidatorTest {
    @Test void rejectsMissingDeltaAdapterBeforeStartingSpark() {
        assertThatThrownBy(() -> GlutenRuntimeValidator.validateDeltaSupport(new ClassLoader(null) {}))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("-Pdelta")
                .hasMessageContaining("DeltaScanTransformer");
    }
    private Properties build(String spark, String scala, String backend) {
        Properties props = new Properties();
        props.setProperty("spark_version", spark);
        props.setProperty("scala_version", scala);
        props.setProperty("backend_type", backend);
        return props;
    }
    @Test void rejectsSparkMinorAndScalaBinaryMismatchesBeforeNativeStartup() {
        assertThatThrownBy(() -> GlutenRuntimeValidator.validateBuild(build("4.1.1", "2.13.17", "velox"), "4.2.0"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("targets Spark 4.1.1");
        assertThatThrownBy(() -> GlutenRuntimeValidator.validateBuild(build("4.2.0", "2.12.18", "velox"), "4.2.0"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Scala 2.13");
        assertThatThrownBy(() -> GlutenRuntimeValidator.validateBuild(build("4.2.0", "2.13.18", "ch"), "4.2.0"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Velox backend");
        GlutenRuntimeValidator.validateBuild(build("4.2.1", "2.13.18", "velox"), "4.2.0");
    }
}
