package com.cascada.spark.adapter.out.spark;

import com.cascada.spark.application.configuration.SparkSessionConfigBuilder;
import com.cascada.spark.domain.SparkSessionConfig;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/** Reject incompatible native startup before Spark creates a context or consumes shared resources. */
final class GlutenRuntimeValidator {
    private GlutenRuntimeValidator() {}

    static void validate(SparkSessionConfig config) {
        boolean enabled = Arrays.stream(config.sparkProperties().getOrDefault("spark.plugins", "").split(","))
                .map(String::trim).anyMatch(SparkSessionConfigBuilder.GLUTEN_PLUGIN::equals);
        if (!enabled) return;
        if (!System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).contains("linux"))
            throw new IllegalArgumentException("Gluten/Velox requires a Linux driver and executors. "
                    + "On Windows use a Linux Docker/WSL runtime, or CASCADA_GLUTEN_ENABLED=false.");
        if (!SparkSessionConfigBuilder.GLUTEN_SHUFFLE.equals(config.sparkProperties().get("spark.shuffle.manager")))
            throw new IllegalArgumentException("Gluten/Velox requires spark.shuffle.manager="
                    + SparkSessionConfigBuilder.GLUTEN_SHUFFLE + " before creating SparkContext");
        boolean deltaExtension = Arrays.stream(config.sparkProperties().getOrDefault("spark.sql.extensions", "").split(","))
                .map(String::trim).anyMatch(SparkSessionConfigBuilder.DELTA_EXTENSIONS::equals);
        if (!deltaExtension)
            throw new IllegalArgumentException("Native Delta requires spark.sql.extensions to include "
                    + SparkSessionConfigBuilder.DELTA_EXTENSIONS);
        if (!Boolean.parseBoolean(config.sparkProperties().getOrDefault("spark.memory.offHeap.enabled", "false")))
            throw new IllegalArgumentException("Gluten/Velox requires spark.memory.offHeap.enabled=true");
        String size = config.sparkProperties().get("spark.memory.offHeap.size");
        if (size == null || org.apache.spark.util.Utils.byteStringAsBytes(size) <= 0)
            throw new IllegalArgumentException("Gluten/Velox requires positive spark.memory.offHeap.size");
        try {
            Class<?> plugin = Class.forName(SparkSessionConfigBuilder.GLUTEN_PLUGIN, false,
                    Thread.currentThread().getContextClassLoader());
            Properties metadata = new Properties();
            try (var input = plugin.getResourceAsStream("/gluten-build-info.properties")) {
                if (input == null) throw new IllegalArgumentException("Gluten bundle has no build metadata; compatibility cannot be verified");
                metadata.load(input);
            }
            validateBuild(metadata, org.apache.spark.package$.MODULE$.SPARK_VERSION());
            validateDeltaSupport(plugin.getClassLoader());
        } catch (ClassNotFoundException unavailable) {
            throw new IllegalArgumentException("Place the matching Gluten/Velox bundle on the driver JVM classpath "
                    + "before starting Cascada; spark.jars or spark.driver.extraClassPath set after JVM startup is insufficient.", unavailable);
        } catch (IOException unreadable) {
            throw new IllegalArgumentException("Cannot read Gluten bundle build metadata", unreadable);
        }
    }

    static void validateDeltaSupport(ClassLoader loader) {
        String[] required = {"org.apache.gluten.execution.DeltaScanTransformer",
                "org.apache.gluten.extension.OffloadDeltaScan",
                "org.apache.gluten.component.VeloxDeltaComponent"};
        try {
            for (String name : required) Class.forName(name, false, loader);
            if (loader.getResource("META-INF/gluten-components/org.apache.gluten.component.VeloxDeltaComponent") == null)
                throw new ClassNotFoundException("VeloxDeltaComponent registration marker");
            Class.forName(SparkSessionConfigBuilder.DELTA_EXTENSIONS, false, loader);
        } catch (ClassNotFoundException | LinkageError missing) {
            throw new IllegalArgumentException("Cascada requires a Delta-enabled Gluten/Velox bundle built with -Pdelta "
                    + "and matching Delta runtime jars on both driver and executors. Missing/incompatible: "
                    + missing.getMessage(), missing);
        }
    }

    static void validateBuild(Properties metadata, String sparkVersion) {
        if (!"velox".equals(metadata.getProperty("backend_type")))
            throw new IllegalArgumentException("The configured Gluten bundle is not the Velox backend");
        String compiledSpark = metadata.getProperty("spark_version", "");
        if (!binaryVersion(compiledSpark).equals(binaryVersion(sparkVersion)))
            throw new IllegalArgumentException("Gluten bundle targets Spark " + compiledSpark
                    + " but this runtime is Spark " + sparkVersion + "; use a matching major/minor bundle");
        if (!metadata.getProperty("scala_version", "").startsWith("2.13."))
            throw new IllegalArgumentException("This runtime requires a Scala 2.13 Gluten bundle");
    }

    private static String binaryVersion(String version) {
        String[] parts = version.split("\\.");
        if (parts.length < 2) throw new IllegalArgumentException("Invalid Spark build version: " + version);
        return parts[0] + "." + parts[1];
    }
}
