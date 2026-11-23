package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Immutable payloads become visible only through the scoped manifest index. */
final class ValkeyExactCubeStore implements ExactCubeStorePort {
    static final String PREFIX = "QC:V4:exact:v1:";
    private static final int MAX_BYTES = 8 * 1024 * 1024;
    private static final int MAX_INDEX_ENTRIES = 4096;
    private static final String PUBLISH = """
            if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
            if not redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3], 'NX') then return 0 end
            redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
            local count = redis.call('ZCARD', KEYS[2])
            if count > tonumber(ARGV[6]) then
                redis.call('ZREMRANGEBYRANK', KEYS[2], 0, count - tonumber(ARGV[6]) - 1)
            end
            redis.call('PEXPIRE', KEYS[2], ARGV[3])
            return 1
            """;
    private static final String READ = """
            if redis.call('GET', KEYS[2]) ~= ARGV[1] then return false end
            if redis.call('ZSCORE', KEYS[3], ARGV[3]) == false then return false end
            if redis.call('STRLEN', KEYS[1]) ~= tonumber(ARGV[2]) then return false end
            return redis.call('GET', KEYS[1])
            """;

    private final RedisCommands<byte[], byte[]> commands;
    private final CacheValueSerializerPort serializer;
    private final Supplier<String> fence;
    private final byte[] fenceKey;

    ValkeyExactCubeStore(RedisCommands<byte[], byte[]> commands, CacheValueSerializerPort serializer,
                         Supplier<String> fence, String fenceKey) {
        this.commands = commands;
        this.serializer = serializer;
        this.fence = fence;
        this.fenceKey = bytes(fenceKey);
    }

    @Override
    public String publicationFence() {
        return fence.get();
    }

    @Override
    public List<ExactCubeFragment> candidates(CubePopulation population, CubeInterval interval, int limit) {
        QueryCancellation.requireCurrentActive();
        String current = publicationFence();
        List<byte[]> descriptors = commands.zrangebyscore(index(population, current),
                Range.from(Range.Boundary.including((double) interval.start()),
                        Range.Boundary.excluding((double) interval.end())), Limit.create(0, limit));
        List<ExactCubeFragment> manifests = new ArrayList<>(descriptors.size());
        for (byte[] descriptor : descriptors) {
            var fragment = parseManifest(population, descriptor, current);
            if (fragment != null) manifests.add(fragment);
        }
        return manifests;
    }

    @Override
    public Optional<ResultFrame> read(ExactCubeFragment manifest) {
        QueryCancellation.requireCurrentActive();
        int separator = manifest.generation().indexOf('/');
        if (separator < 0 || manifest.compressedBytes() <= 0 || manifest.compressedBytes() > MAX_BYTES
                || manifest.decodedBytes() <= 0 || manifest.decodedBytes() > MAX_BYTES) return Optional.empty();
        String generationFence = manifest.generation().substring(0, separator);
        byte[] payload = commands.eval(READ, ScriptOutputType.VALUE,
                new byte[][] {frame(manifest.generation()), fenceKey, index(manifest.population(), generationFence)},
                bytes(generationFence), bytes(Integer.toString(manifest.compressedBytes())),
                bytes(descriptor(manifest)));
        if (payload == null || payload.length < Integer.BYTES || payload.length != manifest.compressedBytes()
                || !digest(payload).equals(manifest.digest())
                || ByteBuffer.wrap(payload).getInt() != manifest.decodedBytes()) return Optional.empty();
        return Optional.of(serializer.deserialize(payload));
    }

    @Override
    public void publish(CubePopulation population, CubeInterval interval, List<String> grain, Set<CubeMeasure> measures,
                        ResultFrame state, Duration ttl, String publicationFence) {
        QueryCancellation.requireCurrentActive();
        byte[] payload = serializer.serialize(state);
        int expanded = ByteBuffer.wrap(payload).getInt();
        if (payload.length > MAX_BYTES || expanded <= 0 || expanded > MAX_BYTES) return;
        String generation = publicationFence + "/" + UUID.randomUUID();
        var manifest = new ExactCubeFragment(generation, population, interval, grain, measures,
                state.rowCount(), payload.length, expanded, digest(payload));
        commands.eval(PUBLISH, ScriptOutputType.INTEGER,
                new byte[][] {frame(generation), index(population, publicationFence), fenceKey},
                bytes(publicationFence), payload, bytes(Long.toString(ttl.toMillis())),
                bytes(Long.toString(interval.start())), bytes(descriptor(manifest)),
                bytes(Integer.toString(MAX_INDEX_ENTRIES)));
    }

    private static byte[] index(CubePopulation population, String fence) {
        var source = population.source();
        var fields = List.of(fence, source.path(), source.tableId(), Long.toString(source.version()), source.schema(),
                source.semantics(), source.accessScope(), population.timeColumn(), population.predicate(), population.input());
        StringBuilder key = new StringBuilder();
        fields.forEach(field -> key.append(field.length()).append(':').append(field));
        return bytes(PREFIX + "index:" + digest(bytes(key.toString())));
    }

    private static byte[] frame(String generation) {
        return bytes(PREFIX + "frame:" + generation);
    }

    private static String descriptor(ExactCubeFragment fragment) {
        return String.join("|", fragment.generation(), Long.toString(fragment.interval().start()),
                Long.toString(fragment.interval().end()), String.join(",", fragment.grain()),
                fragment.measures().stream().sorted().map(Enum::name).collect(Collectors.joining(",")),
                Integer.toString(fragment.rows()), Integer.toString(fragment.compressedBytes()),
                Integer.toString(fragment.decodedBytes()), fragment.digest());
    }

    private static ExactCubeFragment parseManifest(CubePopulation population, byte[] descriptor, String fence) {
        if (descriptor.length > 2048) return null;
        String[] fields = new String(descriptor, StandardCharsets.UTF_8).split("[|]", -1);
        if (fields.length != 9 || !fields[0].startsWith(fence + "/") || !fields[8].matches("[a-f0-9]{64}")) return null;
        try {
            UUID.fromString(fields[0].substring(fence.length() + 1));
            var measures = EnumSet.noneOf(CubeMeasure.class);
            for (String measure : fields[4].split(",")) measures.add(CubeMeasure.valueOf(measure));
            List<String> grain = fields[3].isEmpty() ? List.of() : List.of(fields[3].split(","));
            return new ExactCubeFragment(fields[0], population,
                    new CubeInterval(Long.parseLong(fields[1]), Long.parseLong(fields[2])), grain, measures,
                    Integer.parseInt(fields[5]), Integer.parseInt(fields[6]), Integer.parseInt(fields[7]), fields[8]);
        } catch (IllegalArgumentException | ArithmeticException corrupt) {
            return null;
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String digest(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
