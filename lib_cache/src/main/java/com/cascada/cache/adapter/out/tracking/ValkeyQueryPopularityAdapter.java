package com.cascada.cache.adapter.out.tracking;

import com.cascada.cache.application.port.out.QueryPopularityPort;
import com.cascada.cache.domain.key.CacheKeyConstants;
import com.cascada.cache.domain.model.query.PopularQuery;
import com.cascada.identity.domain.model.QueryHash;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.StringCodec;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

/**
 * Durable Layer-2 popularity in Valkey: {@code QT:V1:TOP} ranks hashes by cumulative hits and
 * {@code QT:V1:META:<hash>} keeps the physical SQL under a sliding retention, so a query unused for longer
 * than the retention drops out of the top-N and popular queries survive restarts.
 *
 * <p>Observations are pipelined fire-and-forget: popularity is a warming hint and must never add a network
 * round trip to the query path.
 */
public final class ValkeyQueryPopularityAdapter implements QueryPopularityPort, AutoCloseable {

    private static final String PHYSICAL_SQL_FIELD = "physical_sql";
    /** Over-read the ranking because members whose metadata expired are skipped and pruned. */
    private static final int EXPIRED_MEMBER_HEADROOM = 3;

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final Duration retention;

    public ValkeyQueryPopularityAdapter(String redisUniformResourceIdentifier, Duration retention) {
        this.retention = Objects.requireNonNull(retention, "retention");
        this.redisClient = RedisClient.create(redisUniformResourceIdentifier);
        this.connection = redisClient.connect(StringCodec.UTF8);
    }

    @Override
    public void recordObservation(QueryHash queryHash, String physicalSql) {
        RedisAsyncCommands<String, String> commands = connection.async();
        String metadataKey = metadataKey(queryHash.value());
        commands.zincrby(CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY, 1, queryHash.value());
        commands.hset(metadataKey, PHYSICAL_SQL_FIELD, physicalSql);
        commands.expire(metadataKey, retention);
    }

    @Override
    public long hitCount(QueryHash queryHash) {
        return Optional.ofNullable(connection.sync().zscore(
                        CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY, queryHash.value()))
                .map(Double::longValue)
                .orElse(0L);
    }

    @Override
    public List<PopularQuery> topByPopularity(int limit) {
        if (limit == 0) {
            return List.of();
        }
        RedisAsyncCommands<String, String> commands = connection.async();
        List<String> rankedHashes = connection.sync().zrevrange(CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY,
                0, (long) limit * EXPIRED_MEMBER_HEADROOM - 1);
        List<CompletableFuture<String>> pendingPhysicalSql = rankedHashes.stream()
                .map(hash -> commands.hget(metadataKey(hash), PHYSICAL_SQL_FIELD).toCompletableFuture())
                .toList();
        List<String> physicalSql = pendingPhysicalSql.stream().map(CompletableFuture::join).toList();
        String[] expiredHashes = IntStream.range(0, rankedHashes.size())
                .filter(index -> physicalSql.get(index) == null)
                .mapToObj(rankedHashes::get)
                .toArray(String[]::new);
        Optional.of(expiredHashes).filter(hashes -> hashes.length > 0)
                .ifPresent(hashes -> commands.zrem(CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY, hashes));
        return IntStream.range(0, rankedHashes.size())
                .filter(index -> physicalSql.get(index) != null)
                .mapToObj(index -> new PopularQuery(new QueryHash(rankedHashes.get(index)), physicalSql.get(index)))
                .limit(limit)
                .toList();
    }

    private static String metadataKey(String queryHash) {
        return CacheKeyConstants.QUERY_TRACKER_META_KEY_PREFIX + ":" + queryHash;
    }

    @Override
    public void close() {
        connection.close();
        redisClient.shutdown();
    }
}
