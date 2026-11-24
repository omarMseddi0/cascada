package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.adapter.out.serialization.PortableFrameSerializer;
import com.cascada.cache.domain.frame.ColumnType;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.ExactCubeSource;
import com.cascada.cache.fixture.ValkeyTestDatabase;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScriptOutputType;
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

    @Test void selectsRarePostingsAndReturnsAllMatchingDescriptors() {
        var source = new ExactCubeSource("facts", "table", 12, "schema", "semantics", "policy", Map.of(), true);
        var population = new CubePopulation(source, "ts", "", "amount");
        var range = new CubeInterval(0, 86400);
        var measure = Set.of(CubeMeasure.SUM);
        var ttl = Duration.ofMinutes(5);
        var state = ResultFrame.builder().column("cube_rows", ColumnType.LONG)
                .column("cube_sum", ColumnType.LONG).row(1L, 2L).build();
        var store = valkey.cache(new PortableFrameSerializer()).exactCubeStore();
        String fence = store.publicationFence();
        int width = Integer.getInteger("cascada.exact.index.fixture.descriptors", 4096);
        for (int i = 0; i < width; i++) {
            store.publish(population, range, List.of("region", "device"), measure, state, ttl, fence);
        }
        store.publish(population, range, List.of("region", "store"), measure, state, ttl, fence);
        store.publish(population, range, List.of("region", "store"), Set.of(CubeMeasure.COUNT), state, ttl, fence);
        var client = RedisClient.create(valkey.uri());
        try (var connection = client.connect()) {
            var commands = connection.sync();
            int stale = Integer.getInteger("cascada.exact.index.fixture.staleDescriptors", 0);
            if (stale > 0) {
                var posting = commands.scan(ScanArgs.Builder.matches("QC:V4:exact:*:SUM:region").limit(10_000));
                assertThat(posting.getKeys()).hasSize(1);
                String key = posting.getKeys().getFirst();
                String insert = """
                        for i = tonumber(ARGV[1]), tonumber(ARGV[2]) do
                            local id = string.format('%08x-0000-4000-8000-%012x', i, i)
                            redis.call('ZADD', KEYS[1], 0, ARGV[3] .. '/' .. id ..
                              '|0|86400|region,device|SUM|1|64|64|' .. string.rep('0', 64))
                        end
                        return 1
                        """;
                for (int start = 1; start <= stale; start += 4096) {
                    commands.eval(insert, ScriptOutputType.INTEGER, new String[] {key},
                            Integer.toString(start), Integer.toString(Math.min(stale, start + 4095)), fence);
                }
                assertThat(commands.zcard(key)).isEqualTo((long) width + stale + 1);
            }
            long keysBefore = calls(commands.info("commandstats"), "keys");
            long scansBefore = calls(commands.info("commandstats"), "scan");
            var shortlist = store.candidates(population, range, List.of("region", "store"), measure);
            assertThat(shortlist).singleElement().satisfies(fragment -> {
                assertThat(fragment.grain()).containsExactly("region", "store");
                assertThat(fragment.measures()).containsExactly(CubeMeasure.SUM);
                assertThat(store.read(fragment)).isPresent();
            });
            assertThat(store.candidates(population, range, List.of("region"), measure)).hasSize(width + stale + 1);
            assertThat(store.candidates(population, range, List.of("unknown"), measure)).isEmpty();
            assertThat(calls(commands.info("commandstats"), "keys")).isEqualTo(keysBefore);
            assertThat(calls(commands.info("commandstats"), "scan")).isEqualTo(scansBefore);
            System.out.println("EXACT_CUBE_VALKEY indexed=" + (width + stale + 1) + " rare=1 broad=" + (width + stale + 1));
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
