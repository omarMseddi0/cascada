package com.cascada.app.bootstrap;

import com.cascada.sql.domain.model.RegisteredTable;
import com.cascada.sql.domain.TableCatalog;
import com.cascada.sql.adapter.out.calcite.LogicalToPhysicalSqlTranslator;
import com.cascada.sql.adapter.out.calcite.CalciteCanonicalObjectFactory;
import com.cascada.app.model.config.WarmingSettings;
import com.cascada.app.model.config.TableSettings;
import com.cascada.app.model.config.EngineSettings;
import com.cascada.app.model.config.ApplicationSettings;
import com.cascada.app.config.CascadaEngineFactory;
import com.cascada.app.config.CacheBackend;
import com.cascada.app.adapter.in.api.QueryResponseEncoder;
import com.cascada.app.adapter.in.api.QueryHttpServer;
import com.cascada.app.adapter.in.api.QueryGrpcService;
import com.cascada.app.adapter.in.api.CacheGrpcService;
import com.cascada.app.adapter.in.api.*;
import com.cascada.app.adapter.in.grpc.protocol.*;
import com.cascada.app.config.*;
import com.cascada.cache.application.model.config.CacheExecutionConfiguration;
import com.cascada.cache.application.service.hashing.QueryHashGenerator;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyFactory;
import com.cascada.cache.domain.model.time.GapPlan;
import com.cascada.spark.adapter.out.spark.SparkDeltaQueryExecutor;
import com.cascada.sql.adapter.out.calcite.*;
import com.cascada.sql.domain.*;
import com.fasterxml.jackson.databind.*;
import io.grpc.*;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.scheduler.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import static org.assertj.core.api.Assertions.*;

/** Real Spark + Delta + Valkey + HTTP/gRPC; every cache answer is compared with plain Spark SQL. */
@EnabledIfEnvironmentVariable(named="CASCADA_TEST_VALKEY_URI", matches=".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ValkeySparkProcessIntegrationTest {
    private static final long DAY=86400, BASE=1759968000L, END=BASE+7*DAY-1;
    private static final ObjectMapper JSON=new ObjectMapper();
    private final AtomicInteger physicalCalls=new AtomicInteger();
    private final Set<String> jobGroups=ConcurrentHashMap.newKeySet();
    private final List<Map<String,Object>> measurements=new ArrayList<>();
    private SparkSession spark, oracle;
    private SparkDeltaQueryExecutor physical, direct;
    private Path fixture;
    private RedisClient inspector;
    private StatefulRedisConnection<String,String> inspection;
    private CascadaEngineFactory factory;
    private QueryHttpServer http;
    private QueryGrpcService grpcQueries;
    private Server grpc;
    private ManagedChannel channel;
    private LogicalToPhysicalSqlTranslator translator;
    private String redisUri, deltaPath;
    private boolean nativeEnabled;
    private final String foreignKey="cascada-validation-foreign-"+UUID.randomUUID();

    @BeforeAll void startRealSessionAndData() throws Exception {
        redisUri=System.getenv("CASCADA_TEST_VALKEY_URI");
        nativeEnabled=Boolean.parseBoolean(System.getenv("CASCADA_GLUTEN_ENABLED"));
        fixture=Files.createTempDirectory("cascada-valkey-integration-");
        var warehouse=Files.createDirectory(fixture.resolve("warehouse"));
        var config=LocalSparkFixtures.localConfig().master("local[2]")
                .appName("cascada-valkey-velox-real-process")
                .withEnvironment(name -> name.equals("CASCADA_GLUTEN_ENABLED")
                        ? Boolean.toString(nativeEnabled) : name.equals("SPARK_MASTER")
                        ? System.getenv().getOrDefault("SPARK_MASTER","local[2]") : System.getenv(name))
                .withProperty("spark.sql.warehouse.dir",warehouse.toUri().toString())
                .withProperty("spark.sql.shuffle.partitions","6")
                .withProperty("spark.default.parallelism","6")
                .withProperty("spark.sql.session.timeZone","UTC")
                .withProperty("spark.databricks.delta.snapshotPartitions","2")
                .withProperty("spark.driver.maxResultSize","128m")
                .withProperty("spark.memory.offHeap.enabled",Boolean.toString(nativeEnabled))
                .build();
        physical=new SparkDeltaQueryExecutor(config);
        spark=SparkSession.active();
        assertThat(spark.version()).isEqualTo("4.1.1");
        spark.sparkContext().setLogLevel("ERROR");
        if (System.getProperty("os.name").startsWith("Windows")) {
            spark.sparkContext().hadoopConfiguration().set("fs.file.impl",LocalSparkConsistencyIntegrationTest.NioLocalFileSystem.class.getName());
            spark.sparkContext().hadoopConfiguration().setBoolean("fs.file.impl.disable.cache",true);
        }
        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override public void onJobStart(SparkListenerJobStart event) {
                String group=event.properties().getProperty("spark.jobGroup.id");
                if(group!=null) jobGroups.add(group);
            }
        });
        // The oracle session's plan must use Spark operators, even when this context hosts Gluten.
        oracle=spark.newSession();
        oracle.conf().set("spark.gluten.enabled","false");
        direct=new SparkDeltaQueryExecutor(oracle);
        long rows=Long.getLong("cascada.consistency.rows",1_000_000L);
        deltaPath=fixture.resolve("events").toUri().toString();
        oracle.sql("""
            SELECT %d + pmod(id*7919,%d) AS ts,
                CASE WHEN id %% 19=0 THEN NULL ELSE concat('r',CAST(id %% 5 AS STRING)) END AS C_region,
                concat('d',CAST(id %% 3 AS STRING)) AS C_device,
                CASE WHEN id %% 11=0 THEN NULL ELSE CAST(id %% 1009-400 AS BIGINT) END AS C_amount,
                CASE WHEN id %% 13=0 THEN NULL ELSE CAST((id %% 997)*0.125 AS DOUBLE) END AS C_latency
            FROM range(0,%d,1,4)
            """.formatted(BASE,7*DAY,rows)).write().format("delta").mode("overwrite").save(deltaPath);
        var catalog=new TableCatalog().register(RegisteredTable.of("traffic",deltaPath,
                Map.of("ts","ts","region","C_region","device","C_device","amount","C_amount","latency","C_latency"),"ts"));
        translator=new LogicalToPhysicalSqlTranslator(300,catalog);
        assertThat(direct.execute("SELECT COUNT(*) AS n FROM delta.`"+deltaPath+"`").longAt(0,0)).isEqualTo(rows);
        inspector=RedisClient.create(redisUri);
        inspection=inspector.connect();
        assertThat(inspection.sync().ping()).isEqualTo("PONG");
        System.out.println("VALKEY_RUNTIME spark="+spark.version()+" rows="+rows+" native="+nativeEnabled+" server="
                +inspection.sync().info("server").replace('\r',' ').replace('\n',' '));
    }

    @BeforeEach void startActualApiAndFactory() throws Exception {
        startApi(true);
    }

    private void startApi(boolean clearBuckets) throws Exception {
        var server=new ServerSettings(0,0,Duration.ofSeconds(30));
        var application=new ApplicationSettings(server,new WarmingSettings(true,Duration.ofHours(1),Duration.ofDays(7),128),
                List.of(new TableSettings("traffic",deltaPath,Map.of("ts","ts","region","C_region","device","C_device",
                        "amount","C_amount","latency","C_latency"),Set.of("ts"))));
        var settings=new EngineSettings(CacheExecutionConfiguration.defaults(),redisUri,"traffic",deltaPath,true,10,CacheBackend.VALKEY);
        factory=new CascadaEngineFactory(settings,sql -> {physicalCalls.incrementAndGet();return physical.execute(sql);},application);
        if(clearBuckets) factory.flushCacheUseCase().flushEverything();
        inspection.sync().set(foreignKey,"preserve-me");
        http=new QueryHttpServer(server,factory.executeLogicalQueryUseCase());
        http.bindCacheAdministration(factory.measureCacheSizeUseCase(),factory.flushCacheUseCase(),factory.warmCacheUseCase());
        http.start();
        grpcQueries=new QueryGrpcService(factory.executeLogicalQueryUseCase(),server);
        grpc=ServerBuilder.forPort(0).addService(grpcQueries).addService(new CacheGrpcService(
                factory.measureCacheSizeUseCase(),factory.flushCacheUseCase(),factory.warmCacheUseCase())).build().start();
        channel=ManagedChannelBuilder.forAddress("127.0.0.1",grpc.getPort()).usePlaintext().build();
    }

    @AfterEach void closeApiAndClearOwnedBuckets() throws Exception {
        if(channel!=null){channel.shutdownNow();channel.awaitTermination(10,TimeUnit.SECONDS);channel=null;}
        if(grpc!=null){grpc.shutdownNow();grpc.awaitTermination(10,TimeUnit.SECONDS);grpc=null;}
        if(grpcQueries!=null){grpcQueries.close();grpcQueries=null;}
        if(http!=null){http.close();http=null;}
        if(factory!=null){
            factory.flushCacheUseCase().flushEverything();
            assertThat(inspection.sync().get(foreignKey)).isEqualTo("preserve-me");
            inspection.sync().del(foreignKey);
            factory.close();factory=null;
        }
    }

    @AfterAll void stopAndCleanFixture() throws Exception {
        String summary=JSON.writeValueAsString(Map.of("nativeEnabled",nativeEnabled,"spark",spark==null?"unavailable":spark.version(),
                "physicalSqlExecutions",physicalCalls.get(),"distinctJobGroups",jobGroups.size(),"measurements",measurements));
        System.out.println("VALKEY_SUMMARY "+summary);
        String output=System.getenv("CASCADA_TEST_REPORT_PATH");
        if(output!=null) Files.writeString(Path.of(output),summary);
        if(inspection!=null){assertThat(inspection.sync().dbsize()).isZero();inspection.close();}
        if(inspector!=null)inspector.shutdown();
        if(direct!=null)direct.close();
        if(physical!=null)physical.close();
        if(spark!=null)spark.stop();
        if(fixture!=null){
            var resolved=fixture.toAbsolutePath().normalize();
            var temp=Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
            if(!resolved.getParent().equals(temp)||!resolved.getFileName().toString().startsWith("cascada-valkey-integration-"))
                throw new IllegalStateException("Unexpected fixture cleanup path");
            try(var files=Files.walk(resolved)){for(var file:files.sorted(Comparator.reverseOrder()).toList())Files.deleteIfExists(file);}
        }
    }

    private String global(long start,long end) {
        return "SELECT region, SUM(amount) AS total, COUNT(*) AS n, COUNT(amount) AS present, MIN(amount) AS lo, MAX(amount) AS hi "
                +"FROM traffic WHERE ts >= "+start+" AND ts <= "+end+" GROUP BY region ORDER BY region";
    }
    private String series(long start,long end) {
        return "SELECT ts, region, SUM(amount) AS total, COUNT(*) AS n FROM traffic WHERE ts >= "+start
                +" AND ts <= "+end+" GROUP BY ts,region ORDER BY ts,region";
    }
    private ResultFrame truth(String logical) {return direct.execute(translator.translateToPhysicalSql(logical));}
    private JsonNode frameJson(ResultFrame frame) throws Exception {
        var output=new java.io.ByteArrayOutputStream();QueryResponseEncoder.json(frame,output);return JSON.readTree(output.toByteArray());
    }
    private JsonNode post(String path,Map<String,Object> body) throws Exception {
        var request=HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+http.port()+path)).timeout(Duration.ofSeconds(180))
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body))).build();
        var response=HttpClient.newHttpClient().send(request,HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(path+" "+response.body()).isEqualTo(200);
        return JSON.readTree(response.body());
    }
    private void same(ResultFrame actual,ResultFrame expected) {
        assertThat(actual.columnNames()).isEqualTo(expected.columnNames());
        assertThat(actual.columnTypes()).isEqualTo(expected.columnTypes());
        assertThat(actual.rows()).containsExactlyElementsOf(expected.rows());
    }
    private void grpcEquals(String sql,JsonNode expected) throws Exception {
        var chunks=QueryServiceGrpc.newBlockingStub(channel).withDeadlineAfter(180,TimeUnit.SECONDS)
                .query(QueryRequest.newBuilder().setSql(sql).setFormat(ResponseFormat.JSON).build());
        var output=new java.io.ByteArrayOutputStream();
        while(chunks.hasNext()) chunks.next().getPayload().writeTo(output);
        assertThat(JSON.readTree(output.toByteArray())).isEqualTo(expected);
    }

    @Test @Order(1) void fullWarmingRepeatForceAndRedisBlobsMatchSpark() throws Exception {
        String sql=global(BASE,END);
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        var warmed=post("/cache/warm",Map.of("start",BASE+123,"end",END-177));
        assertThat(warmed.get("bucketsWarmed").asInt()).isEqualTo(6);
        var backend=factory.cacheBackendForShutdown();
        assertThat(backend.sizeReport().bucketCount()).isEqualTo(6);
        assertThat(backend.sizeReport().totalBytes()).isPositive();
        var canonical=new CalciteCanonicalObjectFactory().canonicalize(translator.translateToPhysicalSql(sql));
        var hash=new QueryHashGenerator().generateQueryHash(canonical,300);
        String tailKey=CacheKeyFactory.buildBucketKey(hash,BASE+6*DAY,DAY);
        assertThat(inspection.sync().exists(tailKey)).isZero();
        String firstKey=CacheKeyFactory.buildBucketKey(hash,BASE,DAY);
        assertThat(inspection.sync().strlen(firstKey)).isPositive();
        String complete=global(BASE,BASE+6*DAY-1);
        int calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(complete).frame(),truth(complete));
        assertThat(physicalCalls.get()).isEqualTo(calls);
        var repeat=post("/cache/warm",Map.of("start",BASE,"end",BASE+6*DAY-1));
        assertThat(repeat.get("bucketsWarmed").asInt()).isZero();
        assertThat(repeat.get("bucketsSkipped").asInt()).isEqualTo(6);
        post("/cache/warm",Map.of("start",BASE,"end",BASE+6*DAY-1,"forceOverwrite",true));
        assertThat(physicalCalls.get()).isEqualTo(calls+6);
        assertThat(inspection.sync().get(foreignKey)).isEqualTo("preserve-me");
        measurements.add(Map.of("case","warming-repeat-force","buckets",6,"bytes",backend.sizeReport().totalBytes()));
        System.out.println("VALKEY_PASS warming-repeat-force-real-blobs");
    }

    @Test @Order(2) void warmedTimeSeriesAndMissingBodyHeadTailMatchSpark() throws Exception {
        String sql=series(BASE,END);
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        int calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        assertThat(physicalCalls.get()).isEqualTo(calls);
        var canonical=new CalciteCanonicalObjectFactory().canonicalize(translator.translateToPhysicalSql(sql));
        var hash=new QueryHashGenerator().generateQueryHash(canonical,300);
        inspection.sync().del(CacheKeyFactory.buildBucketKey(hash,BASE+2*DAY,DAY),CacheKeyFactory.buildBucketKey(hash,BASE+4*DAY,DAY));
        String partial=series(BASE+123,END-177);
        calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(partial).frame(),truth(partial));
        assertThat(physicalCalls.get()).isGreaterThan(calls);
        measurements.add(Map.of("case","time-series-eviction-head-tail","cachedBuckets",factory.measureCacheSizeUseCase().measureCacheSize().bucketCount()));
        System.out.println("VALKEY_PASS time-series-eviction-head-tail");
    }

    @Test @Order(3) void concurrentHttpAndGrpcHotHitsAvoidSparkAndKeepSharedSession() throws Exception {
        String sql=global(BASE,END);
        factory.executeLogicalQueryUseCase().query(sql);
        post("/cache/warm",Map.of("start",BASE,"end",END));
        JsonNode expected=frameJson(truth(sql));
        int calls=physicalCalls.get();
        var latency=concurrent(32,index -> {
            if(index%2==0)assertThat(post("/query",Map.of("sql",sql))).isEqualTo(expected);
            else grpcEquals(sql,expected);
        });
        assertThat(physicalCalls.get()).isEqualTo(calls);
        assertThat(spark.sparkContext().isStopped()).isFalse();
        measurements.add(Map.of("case","32-hot-http-grpc","p50Millis",latency[0],"p95Millis",latency[1],"sparkCalls",0));
        System.out.println("VALKEY_PASS 32-concurrent-hot-http-grpc p50_ms="+latency[0]+" p95_ms="+latency[1]);
    }

    @Test @Order(4) void concurrentPartialMissesAndForcedWarmingRemainConsistent() throws Exception {
        String sql=global(BASE,END);
        factory.executeLogicalQueryUseCase().query(sql);
        post("/cache/warm",Map.of("start",BASE,"end",BASE+DAY-1));
        JsonNode expected=frameJson(truth(sql));
        int calls=physicalCalls.get();
        int groups=jobGroups.size();
        var latency=concurrent(16,index -> {
            if(index%2==0) assertThat(post("/query",Map.of("sql",sql))).isEqualTo(expected);
            else grpcEquals(sql,expected);
        });
        assertThat(physicalCalls.get()).isGreaterThan(calls);
        assertThat(jobGroups.size()).isGreaterThanOrEqualTo(groups+16);
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        post("/cache/warm",Map.of("start",BASE,"end",END));
        concurrent(17,index -> {
            if(index==16)post("/cache/warm",Map.of("start",BASE,"end",END,"forceOverwrite",true));
            else assertThat(post("/query",Map.of("sql",sql))).isEqualTo(expected);
        });
        measurements.add(Map.of("case","16-partial-http-grpc-plus-force-warm","p50Millis",latency[0],"p95Millis",latency[1],
                "distinctGroups",jobGroups.size()-groups));
        System.out.println("VALKEY_PASS concurrent-partial-force-warm p50_ms="+latency[0]+" p95_ms="+latency[1]);
    }

    @Test @Order(5) void flushBothTransportsPreservesForeignKeysAndRecoversColdQueries() throws Exception {
        String sql=global(BASE,END);
        factory.executeLogicalQueryUseCase().query(sql);
        post("/cache/warm",Map.of("start",BASE,"end",END));
        var cache=CacheServiceGrpc.newBlockingStub(channel).withDeadlineAfter(60,TimeUnit.SECONDS);
        assertThat(cache.measureCache(EmptyRequest.getDefaultInstance()).getBucketCount()).isEqualTo(7);
        assertThat(cache.flushCache(FlushCacheRequest.getDefaultInstance()).getBucketsPurged()).isEqualTo(7);
        assertThat(inspection.sync().get(foreignKey)).isEqualTo("preserve-me");
        int calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        assertThat(physicalCalls.get()).isGreaterThan(calls);
        post("/cache/warm",Map.of("start",BASE,"end",END));
        assertThat(post("/cache/flush",Map.of()).get("bucketsPurged").asInt()).isEqualTo(7);
        assertThat(factory.measureCacheSizeUseCase().measureCacheSize().bucketCount()).isZero();
        System.out.println("VALKEY_PASS http-grpc-flush-preserves-foreign");
    }

    @Test @Order(6) void corruptRedisBlobFallsBackAndForcedWarmingRepairsIt() throws Exception {
        String sql=global(BASE,END);
        factory.executeLogicalQueryUseCase().query(sql);
        post("/cache/warm",Map.of("start",BASE,"end",END));
        var canonical=new CalciteCanonicalObjectFactory().canonicalize(translator.translateToPhysicalSql(sql));
        var hash=new QueryHashGenerator().generateQueryHash(canonical,300);
        inspection.sync().set(CacheKeyFactory.buildBucketKey(hash,BASE+DAY,DAY),"invalid-cache-blob");
        int calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        assertThat(physicalCalls.get()).isEqualTo(calls+1);
        post("/cache/warm",Map.of("start",BASE,"end",END,"forceOverwrite",true));
        calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        assertThat(physicalCalls.get()).isEqualTo(calls);
        measurements.add(Map.of("case","corrupt-blob-authoritative-fallback-force-repair","recovered",true));
        System.out.println("VALKEY_PASS corrupt-blob-fallback-force-repair");
    }

    @Test @Order(7) void freshFactoryReusesPersistentRedisBucketsWithoutItsOldCoverageIndex() throws Exception {
        String sql=global(BASE,END);
        factory.executeLogicalQueryUseCase().query(sql);
        post("/cache/warm",Map.of("start",BASE,"end",END));
        channel.shutdownNow();channel.awaitTermination(10,TimeUnit.SECONDS);channel=null;
        grpc.shutdownNow();grpc.awaitTermination(10,TimeUnit.SECONDS);grpc=null;
        grpcQueries.close();grpcQueries=null;
        http.close();http=null;
        factory.close();factory=null;
        startApi(false);
        int calls=physicalCalls.get();
        same(factory.executeLogicalQueryUseCase().query(sql).frame(),truth(sql));
        assertThat(physicalCalls.get()).isEqualTo(calls);
        assertThat(factory.measureCacheSizeUseCase().measureCacheSize().bucketCount()).isEqualTo(7);
        measurements.add(Map.of("case","new-factory-persistent-redis-reuse","sparkCalls",0));
        System.out.println("VALKEY_PASS factory-reconnect-persistent-buckets");
    }

    @Test @Order(8) void physicalPlanProvesNativeOffloadWhenRequestedAndPlainSparkOracle() {
        String sql=translator.translateToPhysicalSql(global(BASE,END));
        var original=oracle.sql(sql);original.collectAsList();
        String referencePlan=original.queryExecution().executedPlan().toString();
        assertThat(referencePlan).doesNotContain("Transformer");
        var accelerated=spark.sql(sql);accelerated.collectAsList();
        String plan=accelerated.queryExecution().executedPlan().toString();
        System.out.println("VALKEY_PLAN_CONFIG extensions="+spark.conf().get("spark.sql.extensions","")
                +" enabled="+spark.conf().get("spark.gluten.enabled","true")
                +" threadEnabled="+spark.sparkContext().getLocalProperty("gluten.enabledForCurrentThread"));
        if(nativeEnabled){
            var basic=spark.sql("SELECT SUM(id) AS n FROM range(100000)");basic.collectAsList();
            System.out.println("VALKEY_BASIC_PLAN\n"+basic.queryExecution().executedPlan());
        }
        System.out.println("VALKEY_EXECUTED_PLAN native="+nativeEnabled+"\n"+plan);
        List<String> executedClasses=executedClasses(accelerated.queryExecution().executedPlan());
        long nativeStages=executedClasses.stream().filter(name -> name.contains("WholeStageTransformer")).count();
        if(nativeEnabled){
            assertThat(nativeStages).as("actual native stage classes in the executed AQE plan").isPositive();
            assertThat(executedClasses).anyMatch(name -> name.contains("HashAggregate") && name.contains("Transformer"));
            assertThat(executedClasses).as("native scan of the real Delta data table")
                    .anyMatch(name -> name.endsWith("DeltaScanTransformer"));
        }
        System.out.println("VALKEY_NATIVE_EXECUTION_CLASSES "+executedClasses);
        measurements.add(Map.of("case","executed-plan","nativeTransformer",nativeStages>0,"nativeStages",nativeStages));
    }

    @Test @Order(9) void complexSqlMatrixMatchesPlainSparkAndRecordsNativeCoverage() {
        spark.read().format("delta").load(deltaPath).createOrReplaceTempView("matrix_events");
        oracle.read().format("delta").load(deltaPath).createOrReplaceTempView("matrix_events");
        String base=" FROM matrix_events WHERE ts >= "+BASE+" AND ts <= "+END;
        var cases=new LinkedHashMap<String,String>();
        cases.put("group-all-aggregates","SELECT C_region, SUM(C_amount) AS total, COUNT(*) AS n, COUNT(C_amount) AS nonnull, MIN(C_amount) AS lo, MAX(C_amount) AS hi, AVG(C_latency) AS mean"+base+" GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("epoch-second-buckets","SELECT CAST(FLOOR(ts/3600)*3600 AS BIGINT) AS slot, C_region, SUM(C_amount) AS total"+base+" GROUP BY CAST(FLOOR(ts/3600)*3600 AS BIGINT), C_region ORDER BY slot, C_region NULLS FIRST");
        cases.put("distinct-aggregates","SELECT C_region, COUNT(DISTINCT C_amount) AS n, SUM(DISTINCT C_amount) AS total"+base+" GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("decimal-arithmetic","SELECT C_region, SUM(CAST(C_amount AS DECIMAL(20,3))*CAST(0.127 AS DECIMAL(8,3))) AS total"+base+" GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("having-order-offset","SELECT C_region, SUM(C_amount) AS total, COUNT(*) AS n"+base+" GROUP BY C_region HAVING COUNT(*)>10 ORDER BY total DESC, C_region NULLS FIRST LIMIT 3 OFFSET 1");
        cases.put("null-case-filter","SELECT C_device, SUM(CASE WHEN C_region IS NULL THEN C_amount ELSE 0 END) AS total, COUNT(CASE WHEN C_amount IS NULL THEN 1 END) AS missing"+base+" GROUP BY C_device ORDER BY C_device");
        cases.put("union-subquery","SELECT C_region, SUM(n) AS n FROM (SELECT C_region, COUNT(*) AS n"+base+" GROUP BY C_region UNION ALL SELECT C_region, COUNT(*) AS n"+base+" GROUP BY C_region) u GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("uncorrelated-in","SELECT C_region, COUNT(*) AS n"+base+" AND C_device IN (SELECT C_device FROM matrix_events GROUP BY C_device HAVING COUNT(*)>10) GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("correlated-exists","SELECT e.C_region, COUNT(*) AS n FROM matrix_events e WHERE EXISTS (SELECT 1 FROM matrix_events d WHERE d.C_device=e.C_device AND d.C_region='r1') GROUP BY e.C_region ORDER BY e.C_region NULLS FIRST");
        String daily="WITH daily AS (SELECT CAST(FLOOR(ts/86400)*86400 AS BIGINT) AS day, C_region, C_device, SUM(C_amount) AS total, COUNT(*) AS n"+base+" GROUP BY CAST(FLOOR(ts/86400)*86400 AS BIGINT), C_region, C_device) ";
        cases.put("cte-scalar-subquery",daily+"SELECT C_region, SUM(total) AS total, (SELECT COUNT(*) FROM daily) AS groups FROM daily GROUP BY C_region ORDER BY C_region NULLS FIRST");
        cases.put("cte-null-safe-left-join",daily+"SELECT a.C_region, SUM(a.total) AS total, MAX(b.n) AS n FROM daily a LEFT JOIN (SELECT C_region, SUM(n) AS n FROM daily GROUP BY C_region) b ON a.C_region <=> b.C_region GROUP BY a.C_region ORDER BY a.C_region NULLS FIRST");
        cases.put("nested-cte-window",daily+", ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY C_region ORDER BY total DESC, day, C_device) AS pos, SUM(total) OVER (PARTITION BY C_region ORDER BY day, C_device ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM daily) SELECT C_region, C_device, SUM(total) AS total, MAX(running) AS running FROM ranked WHERE pos<=5 GROUP BY C_region,C_device HAVING COUNT(*)>=1 ORDER BY total DESC, C_region NULLS FIRST, C_device LIMIT 10");
        cases.put("grouping-sets", "SELECT C_region, C_device, GROUPING_ID(C_region,C_device) AS gid, SUM(C_amount) AS total"+base+" GROUP BY GROUPING SETS ((C_region,C_device),(C_region),()) ORDER BY gid,C_region NULLS FIRST,C_device NULLS FIRST");
        cases.put("derived-having-topk", "SELECT C_region,SUM(total) AS total,MAX(hi) AS hi FROM (SELECT C_region,C_device,SUM(C_amount) AS total,MAX(C_amount) AS hi"+base+" GROUP BY C_region,C_device HAVING COUNT(*)>10) d GROUP BY C_region ORDER BY total DESC,C_region NULLS FIRST LIMIT 5");
        for(var entry:cases.entrySet()) {
            var reference=oracle.sql(entry.getValue());
            var expected=reference.collectAsList();
            assertThat(executedClasses(reference.queryExecution().executedPlan())).noneMatch(name -> name.contains("Transformer"));
            var candidate=spark.sql(entry.getValue());
            var actual=candidate.collectAsList();
            assertThat(candidate.schema()).as(entry.getKey()+" schema").isEqualTo(reference.schema());
            assertThat(actual).as(entry.getKey()+" rows").hasSize(expected.size());
            for(int row=0;row<expected.size();row++)for(int column=0;column<expected.get(row).size();column++) {
                Object wanted=expected.get(row).get(column), got=actual.get(row).get(column);
                if(wanted instanceof Double number && got instanceof Double result)
                    assertThat(result).as(entry.getKey()+" numeric cell").isCloseTo(number,within(Math.max(1e-9,Math.abs(number)*1e-9)));
                else assertThat(got).as(entry.getKey()+" cell").isEqualTo(wanted);
            }
            var classes=executedClasses(candidate.queryExecution().executedPlan());
            long stages=classes.stream().filter(name -> name.contains("WholeStageTransformer")).count();
            measurements.add(Map.of("case","sql-matrix-"+entry.getKey(),"rows",actual.size(),"nativeStages",stages,"operators",classes));
            System.out.println("VALKEY_SQL_MATRIX_PASS "+entry.getKey()+" nativeStages="+stages);
        }
        assertThat(spark.conf().get("spark.sql.ansi.enabled")).isEqualTo("false");
    }

    private List<String> executedClasses(org.apache.spark.sql.execution.SparkPlan root) {
        var seen=Collections.newSetFromMap(new IdentityHashMap<org.apache.spark.sql.execution.SparkPlan,Boolean>());
        ArrayDeque<org.apache.spark.sql.execution.SparkPlan> pending=new ArrayDeque<>();
        pending.add(root);
        List<String> classes=new ArrayList<>();
        while(!pending.isEmpty()){
            var node=pending.removeFirst();if(!seen.add(node))continue;
            classes.add(node.getClass().getName());
            if(node instanceof org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec adaptive) pending.add(adaptive.executedPlan());
            if(node instanceof org.apache.spark.sql.execution.adaptive.QueryStageExec stage) pending.add(stage.plan());
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.children()));
            pending.addAll(scala.jdk.javaapi.CollectionConverters.asJava(node.subqueries()));
            if(node instanceof org.apache.spark.sql.execution.exchange.ReusedExchangeExec reused) pending.add(reused.child());
        }
        return classes;
    }

    private interface ConcurrentAction {void run(int index) throws Exception;}
    private double[] concurrent(int count,ConcurrentAction action) throws Exception {
        CountDownLatch entered=new CountDownLatch(count),release=new CountDownLatch(1);
        long[] elapsed=new long[count];
        try(var workers=Executors.newVirtualThreadPerTaskExecutor()){
            List<Future<?>> results=new ArrayList<>();
            for(int index=0;index<count;index++){
                int current=index;
                results.add(workers.submit(() -> {
                    entered.countDown();release.await();long start=System.nanoTime();
                    action.run(current);elapsed[current]=System.nanoTime()-start;return null;
                }));
            }
            assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();release.countDown();
            for(var result:results)result.get(240,TimeUnit.SECONDS);
        }
        Arrays.sort(elapsed);
        return new double[]{elapsed[count/2]/1_000_000.0,elapsed[Math.min(count-1,(int)Math.ceil(count*.95)-1)]/1_000_000.0};
    }
}
