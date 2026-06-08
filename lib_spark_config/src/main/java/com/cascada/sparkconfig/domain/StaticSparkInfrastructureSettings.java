package com.cascada.sparkconfig.domain;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The <em>static</em> half of {@code data_collector/src/spark.json}: every key that is fixed
 * infrastructure (Kubernetes placement, HDFS short-circuit reads, Delta auto-optimize, metrics sink,
 * jars, compression, HDFS/Hadoop permissions, executor pod template, API timeouts) rather than derived
 * from the three knobs. The {@link SparkConfigurationDeriver} owns the workload-tuned keys (executor
 * memory/cores, parallelism, AQE, dynamic-allocation bounds, Delta file sizing); this class owns
 * everything else, so together they reproduce the full {@code spark.json} the Python
 * {@code spark_manager.get_full_config()} assembles.
 *
 * <p>Section order mirrors {@code get_full_config()} (common → pod template → hdfs → hadoop
 * permissions → metrics → dynamic-allocation extras → arrow extra → api → compression → additional);
 * the derived keys from each section are intentionally absent here so the two maps are disjoint and the
 * union is exactly the golden file.
 */
public final class StaticSparkInfrastructureSettings {

    private StaticSparkInfrastructureSettings() {
    }

    /** The fixed infrastructure key/value pairs (everything {@link SparkConfigurationDeriver} does not emit). */
    public static Map<String, String> referenceInfrastructure() {
        Map<String, String> entries = new LinkedHashMap<>();

        // --- common (infrastructure subset; executor/driver memory+cores are derived) ---
        entries.put("spark.executor.instances", "3");
        entries.put("spark.submit.deployMode", "client");
        entries.put("spark.driver.host", "spark-master.default.svc.cluster.local");
        entries.put("spark.driver.port", "8002");
        entries.put("spark.blockManager.port", "8001");
        entries.put("spark.kubernetes.namespace", "default");
        entries.put("spark.kubernetes.container.image", "docker.registry.local:5000/spark:python3-java17-hadoop");
        entries.put("spark.kubernetes.container.image.pullPolicy", "IfNotPresent");
        entries.put("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
        entries.put("spark.kubernetes.authenticate.executor.serviceAccountName", "spark");
        entries.put("spark.authenticate", "false");
        entries.put("spark.hadoop.security.authentication", "simple");
        entries.put("spark.hadoop.security.authorization", "false");
        entries.put("spark.driver.bindAddress", "0.0.0.0");
        entries.put("spark.scheduler.mode", "FAIR");

        // --- executor pod template ---
        entries.put("spark.kubernetes.executor.podTemplateFile", "/opt/spark/templates/executor-pod-template.yaml");

        // --- hdfs ---
        entries.put("spark.hadoop.dfs.client.read.shortcircuit", "true");
        entries.put("spark.hadoop.dfs.client.use.datanode.hostname", "false");
        entries.put("spark.hadoop.dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket/dn.sock");
        entries.put("spark.hadoop.dfs.block.local-path-access.user", "hadoop,root");
        entries.put("spark.hadoop.fs.hdfs.impl.disable.cache", "true");
        entries.put("spark.hadoop.dfs.replication", "1");

        // --- hadoop permissions ---
        entries.put("spark.hadoop.fs.permissions.umask-mode", "000");
        entries.put("spark.hadoop.dfs.permissions", "false");
        entries.put("spark.hadoop.fs.permission.umask-mode", "000");

        // --- metrics (graphite sink) ---
        entries.put("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink");
        entries.put("spark.metrics.conf.*.sink.graphite.host", "spark-dashboard-influx.default.svc.cluster.local");
        entries.put("spark.metrics.conf.*.sink.graphite.port", "2003");
        entries.put("spark.metrics.conf.*.sink.graphite.period", "5");
        entries.put("spark.metrics.conf.*.sink.graphite.unit", "seconds");
        entries.put("spark.metrics.conf.*.sink.graphite.prefix", "spark");
        entries.put("spark.metrics.appStatusSource.enabled", "true");
        entries.put("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource");
        entries.put("spark.metrics.namespace", "omar_mseddi");

        // --- dynamic allocation extras (enabled/min/max are derived) ---
        entries.put("spark.dynamicAllocation.shuffleTracking.enabled", "true");
        entries.put("spark.dynamicAllocation.executorIdleTimeout", "60s");
        entries.put("spark.dynamicAllocation.schedulerBacklogTimeout", "60s");

        // --- arrow extra (pyspark.enabled is derived) ---
        entries.put("spark.sql.hive.filesourcePartitionFileCacheSize", "924288000");

        // --- api ---
        entries.put("spark.kubernetes.executor.apiPollingInterval", "65s");
        entries.put("spark.kubernetes.driver.connectionTimeout", "70000");

        // --- compression ---
        entries.put("spark.sql.parquet.compression.codec", "gzip");
        entries.put("spark.hadoop.io.compression.codec.gz.level", "9");

        // --- additional ---
        entries.put("spark.jars",
                "/app/jars/antlr4-runtime-4.9.3.jar,/app/jars/delta-spark_2.12-3.3.0.jar,/app/jars/delta-storage-3.3.0.jar");
        entries.put("spark.sql.statistics.size.autoUpdate.enabled", "true");
        entries.put("spark.sql.join.preferSortMergeJoin", "true");
        entries.put("spark.sql.parquet.mergeSchema", "true");
        entries.put("spark.sql.files.ignoreMissingFiles", "true");
        entries.put("spark.databricks.optimizer.dynamicFilePruning", "true");
        entries.put("spark.sql.files.ignoreCorruptFiles", "true");
        entries.put("spark.driver.maxResultSize", "6g");
        entries.put("fs.permissions.umask-mode", "000");
        entries.put("hadoop.fs.permissions.umask-mode", "000");
        entries.put("fs.hdfs.impl.disable.cache", "true");
        entries.put("spark.databricks.delta.schema.autoMerge.enabled", "true");
        entries.put("spark.databricks.delta.retentionDurationCheck.enabled", "false");
        entries.put("spark.databricks.delta.autoOptimize.optimizeWrite", "true");
        entries.put("spark.databricks.delta.autoOptimize.autoCompact", "true");
        entries.put("spark.kubernetes.executor.resources.requests.ephemeral-storage", "30Gi");
        entries.put("spark.kubernetes.executor.resources.limits.ephemeral-storage", "30Gi");

        return entries;
    }
}
