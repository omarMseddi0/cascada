package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import io.lettuce.core.RedisClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = ValkeyTestDatabase.ENVIRONMENT_VARIABLE, matches = ".+")
class ValkeyExactCubeIndexIntegrationTest {
    @RegisterExtension final ValkeyTestDatabase valkey = new ValkeyTestDatabase();

    @Test void indexesAndReadsTheSameOrdinaryBucket() {
        var source = new ExactCubeSource("facts", "table", 12, "schema", "semantics", "policy", Map.of(), true);
        var population = new CubePopulation(source, "ts", "", "amount");
        var range = new CubeInterval(0, 86400);
        var measure = Set.of(CubeMeasure.SUM);
        var ttl = Duration.ofMinutes(5);
        var state = ResultFrame.builder().column("cube_rows", ColumnType.LONG)
                .column("cube_sum", ColumnType.LONG).row(1L, 2L).build();
        var backend = valkey.cache(new PortableFrameSerializer());
        var store = backend.exactCubeStore();
        String fence = backend.warmingGeneration().orElseThrow();
        String common = "QC:V4:B86400:common:0";
        String rare = "QC:V4:B86400:rare:0";
        assertThat(store.storeBucket(common, state, ttl, fence, population, range, List.of("region", "device"), "mean")).isTrue();
        assertThat(store.storeBucket(rare, state, ttl, fence, population, range, List.of("region", "store"), "mean")).isTrue();
        var client = RedisClient.create(valkey.uri());
        try (var connection = client.connect()) {
            var commands = connection.sync();
            assertThat(commands.keys("QC:V4:B*")).containsExactlyInAnyOrder(common, rare);
            assertThat(commands.keys("QC:V4:exact:v1:frame:*")).isEmpty();
            long keysBefore = calls(commands.info("commandstats"), "keys");
            long scansBefore = calls(commands.info("commandstats"), "scan");
            var shortlist = store.candidates(population, range, List.of("region", "store"), measure);
            assertThat(shortlist).singleElement().satisfies(fragment -> assertThat(fragment.bucketKey()).isEqualTo(rare));
            store.visitBuckets(shortlist, (index, result) -> assertThat(result).hasValueSatisfying(
                    frame -> assertThat(frame.longAt(0, 1)).isEqualTo(2)));
            assertThat(store.candidates(population, range, List.of("region"), measure)).hasSize(2);
            assertThat(store.candidates(population, range, List.of("unknown"), measure)).isEmpty();
            assertThat(calls(commands.info("commandstats"), "keys")).isEqualTo(keysBefore);
            assertThat(calls(commands.info("commandstats"), "scan")).isEqualTo(scansBefore);
        } finally {
            client.shutdown();
        }
    }

    @Test void looksUpDisjointTimeRangesInOneValkeyIndexRequest() {
        var source = new ExactCubeSource("facts", "table", 12, "schema", "semantics", "policy", Map.of(), true);
        var population = new CubePopulation(source, "ts", "", "amount");
        var backend = valkey.cache(new PortableFrameSerializer());
        var store = backend.exactCubeStore();
        String generation = backend.warmingGeneration().orElseThrow();
        var grain = List.of("region", "device");
        var frame = ResultFrame.builder().column("cube_sum", ColumnType.LONG).row(2L).build();
        for (long day = 0; day < 3; day++) {
            long start = day * 86400L;
            assertThat(store.storeBucket("QC:V4:B86400:family:" + start, frame, Duration.ofMinutes(5),
                    generation, population, new CubeInterval(start, start + 86400L), grain, "mean")).isTrue();
        }

        var client = RedisClient.create(valkey.uri());
        try (var connection = client.connect()) {
            var commands = connection.sync();
            long evalBefore = calls(commands.info("commandstats"), "eval");
            var first = new CubeInterval(0, 86400);
            var last = new CubeInterval(172800, 259200);
            assertThat(store.candidates(population, List.of(first, last), grain, Set.of(CubeMeasure.SUM)))
                    .extracting(ExactCubeFragment::interval).containsExactly(first, last);
            assertThat(calls(commands.info("commandstats"), "eval")).isEqualTo(evalBefore + 1);
        } finally {
            client.shutdown();
        }
    }

    private static long calls(String info, String command) {
        String prefix = "cmdstat_" + command + ":calls=";
        return info.lines().filter(line -> line.startsWith(prefix)).findFirst()
                .map(line -> Long.parseLong(line.substring(prefix.length(), line.indexOf(','))))
                .orElse(0L);
    }
}
