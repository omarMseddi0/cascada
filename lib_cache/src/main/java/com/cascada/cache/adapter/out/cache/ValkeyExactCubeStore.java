package com.cascada.cache.adapter.out.cache;

import com.cascada.cache.application.execution.QueryCancellation;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.application.port.out.ExactCubeStorePort;
import com.cascada.cache.domain.frame.ResultFrame;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.model.cube.ExactCubeFragment;
import com.cascada.cache.domain.model.cube.CubePopulation;
import com.cascada.cache.domain.model.cube.CubeInterval;
import com.cascada.cache.domain.model.cube.CubeMeasure;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Scoped shape index referring to the ordinary bucket payloads. */
final class ValkeyExactCubeStore implements ExactCubeStorePort {
    static final String PREFIX = "QC:V4:exact:v1:";
    private static final String POSTINGS = ":bucket-postings:";
    private static final String PUBLISH = """
            if redis.call('GET', KEYS[2]) ~= ARGV[1] then return 0 end
            redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[3])
            redis.call('SET', KEYS[3], ARGV[6])
            for i = 4, #KEYS do
                redis.call('ZADD', KEYS[i], ARGV[4], ARGV[5])
                redis.call('PEXPIRE', KEYS[i], ARGV[3], 'GT')
                if redis.call('PTTL', KEYS[i]) == -1 then redis.call('PEXPIRE', KEYS[i], ARGV[3]) end
            end
            return 1
            """;
    private static final String CANDIDATES = """
            local result = {}
            local width = tonumber(ARGV[3])
            for first = 1, #KEYS, width do
                local last = first + width - 1
                local selected = first
                local smallest = redis.call('ZCOUNT', KEYS[first], ARGV[1], ARGV[2])
                for i = first + 1, last do
                    if smallest == 0 then break end
                    local size = redis.call('ZCOUNT', KEYS[i], ARGV[1], ARGV[2])
                    if size < smallest then selected = i; smallest = size end
                end
                if smallest > 0 then
                    local window = redis.call('ZRANGEBYSCORE', KEYS[selected], ARGV[1], ARGV[2])
                    for _, descriptor in ipairs(window) do
                        local matches = true
                        for i = first, last do
                            if i ~= selected and not redis.call('ZSCORE', KEYS[i], descriptor) then
                                matches = false
                                break
                            end
                        end
                        if matches then
                            result[#result + 1] = descriptor
                        end
                    end
                end
            end
            return result
            """;
    private static final String FORGET_REFERENCE = """
            for i = 4, #KEYS do redis.call('ZREM', KEYS[i], ARGV[1]) end
            return 1
            """;
    private final ValkeyCacheBackendAdapter backend;
    private final RedisCommands<byte[], byte[]> commands;
    private final CacheValueSerializerPort serializer;
    private final Supplier<String> fence;
    private final byte[] fenceKey;
    private final byte[] revisionKey;

    ValkeyExactCubeStore(ValkeyCacheBackendAdapter backend, RedisCommands<byte[], byte[]> commands,
                         CacheValueSerializerPort serializer, Supplier<String> fence, String fenceKey, String revisionKey) {
        this.backend = backend;
        this.commands = commands;
        this.serializer = serializer;
        this.fence = fence;
        this.fenceKey = bytes(fenceKey);
        this.revisionKey = bytes(revisionKey);
    }

    @Override
    public List<ExactCubeFragment> candidates(CubePopulation population, CubeInterval interval,
                                             List<String> grain, Set<CubeMeasure> measures) {
        QueryCancellation.requireCurrentActive();
        String current = fence.get();
        String index = index(population, current);
        // Scores are doubles. Widen by the rounding unit and check exact endpoints below.
        double start = interval.start(), end = interval.end();
        byte[] low = bytes(Double.toString(start - Math.max(1, Math.ulp(start))));
        byte[] high = bytes(Double.toString(end + Math.max(1, Math.ulp(end))));
        var keys = new ArrayList<byte[]>();
        for (CubeMeasure measure : CubeMeasure.values()) {
            if (!measures.contains(measure)) continue;
            if (grain.isEmpty()) keys.add(posting(index, measure, ""));
            else grain.forEach(column -> keys.add(posting(index, measure, column)));
        }
        if (keys.isEmpty()) return List.of();
        List<byte[]> descriptors = commands.eval(CANDIDATES, ScriptOutputType.MULTI,
                keys.toArray(byte[][]::new), low, high,
                bytes(Integer.toString(Math.max(1, grain.size()))));
        var manifests = new LinkedHashSet<ExactCubeFragment>();
        for (byte[] descriptor : descriptors) {
            var fragment = parseManifest(population, descriptor, current);
            if (fragment != null && interval.contains(fragment.interval()) && fragment.grain().containsAll(grain)) {
                manifests.add(fragment);
            }
        }
        return List.copyOf(manifests);
    }

    @Override
    public void visitBuckets(List<ExactCubeFragment> manifests, BiConsumer<Integer, Optional<ResultFrame>> visitor) {
        String generation = fence.get() + "/";
        if (manifests.stream().anyMatch(manifest -> !manifest.generation().startsWith(generation))) {
            throw new IllegalStateException("bucket generation changed before read");
        }
        backend.visitVerifiedKeys(manifests.stream().map(ExactCubeFragment::bucketKey).toList(), (i, payload) -> {
            var manifest = manifests.get(i);
            return payload.length == manifest.compressedBytes() && digest(payload).equals(manifest.digest());
        }, (i, result) -> {
            if (result.isEmpty()) {
                var manifest = manifests.get(i);
                String token = manifest.generation();
                String bucketFence = token.substring(0, token.indexOf('/'));
                commands.eval(FORGET_REFERENCE, ScriptOutputType.INTEGER, manifestKeys(manifest, bucketFence),
                        bytes(descriptor(manifest)));
            }
            visitor.accept(i, result);
        });
    }

    @Override
    public boolean storeBucket(String key, ResultFrame frame, Duration ttl, String generation,
                               CubePopulation population, CubeInterval interval, List<String> grain, String averageAlias) {
        QueryCancellation.requireCurrentActive();
        if (!CacheKeyConstants.isBucketKey(key)) throw new IllegalArgumentException("expected an ordinary bucket key");
        byte[] payload = serializer.serialize(frame);
        int expanded = ByteBuffer.wrap(payload).getInt();
        var manifest = new ExactCubeFragment(generation + "/" + UUID.randomUUID(), population, interval, grain,
                EnumSet.allOf(CubeMeasure.class), frame.rowCount(), payload.length, expanded, digest(payload),
                key, averageAlias);
        Long stored = commands.eval(PUBLISH, ScriptOutputType.INTEGER, manifestKeys(manifest, generation),
                bytes(generation), payload, bytes(Long.toString(ttl.toMillis())),
                bytes(Long.toString(interval.start())), bytes(descriptor(manifest)), bytes(UUID.randomUUID().toString()));
        return Long.valueOf(1L).equals(stored);
    }

    private static String index(CubePopulation population, String fence) {
        var source = population.source();
        var fields = List.of(fence, source.path(), source.tableId(), Long.toString(source.version()), source.schema(),
                source.semantics(), source.accessScope(), population.timeColumn(), population.predicate(), population.input());
        StringBuilder key = new StringBuilder();
        fields.forEach(field -> key.append(field.length()).append(':').append(field));
        return PREFIX + "index:" + digest(bytes(key.toString())) + POSTINGS;
    }

    private byte[][] manifestKeys(ExactCubeFragment manifest, String fence) {
        String index = index(manifest.population(), fence);
        var keys = new ArrayList<byte[]>();
        keys.add(bytes(manifest.bucketKey()));
        keys.add(fenceKey);
        keys.add(revisionKey);
        for (CubeMeasure measure : manifest.measures().stream().sorted().toList()) {
            keys.add(posting(index, measure, ""));
            manifest.grain().forEach(column -> keys.add(posting(index, measure, column)));
        }
        return keys.toArray(byte[][]::new);
    }

    private static byte[] posting(String index, CubeMeasure measure, String column) {
        return bytes(index + measure.name() + ":" + column);
    }

    private static String descriptor(ExactCubeFragment fragment) {
        return String.join("|", fragment.generation(), Long.toString(fragment.interval().start()),
                Long.toString(fragment.interval().end()), String.join(",", fragment.grain()),
                fragment.measures().stream().sorted().map(Enum::name).collect(Collectors.joining(",")),
                Integer.toString(fragment.rows()), Integer.toString(fragment.compressedBytes()),
                Integer.toString(fragment.decodedBytes()), fragment.digest(), fragment.bucketKey(), fragment.averageAlias());
    }

    private static ExactCubeFragment parseManifest(CubePopulation population, byte[] descriptor, String fence) {
        String[] fields = new String(descriptor, StandardCharsets.UTF_8).split("[|]", -1);
        if (fields.length != 11 || !fields[0].startsWith(fence + "/") || !fields[8].matches("[a-f0-9]{64}")
                || !CacheKeyConstants.isBucketKey(fields[9])) return null;
        try {
            UUID.fromString(fields[0].substring(fence.length() + 1));
            var measures = EnumSet.noneOf(CubeMeasure.class);
            for (String measure : fields[4].split(",")) measures.add(CubeMeasure.valueOf(measure));
            List<String> grain = fields[3].isEmpty() ? List.of() : List.of(fields[3].split(","));
            return new ExactCubeFragment(fields[0], population,
                    new CubeInterval(Long.parseLong(fields[1]), Long.parseLong(fields[2])), grain, measures,
                    Integer.parseInt(fields[5]), Integer.parseInt(fields[6]), Integer.parseInt(fields[7]), fields[8], fields[9], fields[10]);
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
