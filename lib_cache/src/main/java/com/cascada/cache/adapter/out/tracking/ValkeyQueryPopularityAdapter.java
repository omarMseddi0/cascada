package com.cascada.cache.adapter.out.tracking;

import com.cascada.cache.adapter.out.tracking.model.PopularityObservation;
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
 * <p>Observations accumulate in a bounded in-memory buffer. A background thread pipelines one
 * update per hash every ten seconds; the request path never performs Redis I/O.
 */
public final class ValkeyQueryPopularityAdapter implements QueryPopularityPort, AutoCloseable {

    private static final String PHYSICAL_SQL_FIELD = "physical_sql";
    /** Over-read the ranking because members whose metadata expired are skipped and pruned. */
    private static final int EXPIRED_MEMBER_HEADROOM = 3;

    private final RedisClient redisClient;
    private final StatefulRedisConnection<String, String> connection;
    private final Duration retention;
    private final java.util.Map<QueryHash, PopularityObservation> pending = new java.util.LinkedHashMap<>();
    private final Object flushLock = new Object();
    private final java.util.concurrent.ScheduledExecutorService flusher;
    private volatile boolean closed;
    private static final int MAX_PENDING = 10_000;
    private static final java.util.logging.Logger LOGGER = java.util.logging.Logger.getLogger(ValkeyQueryPopularityAdapter.class.getName());

    public ValkeyQueryPopularityAdapter(String redisUniformResourceIdentifier, Duration retention) {
        this.retention = Objects.requireNonNull(retention, "retention");
        this.redisClient = RedisClient.create(redisUniformResourceIdentifier);
        this.connection = redisClient.connect(StringCodec.UTF8);
        this.flusher = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon(true).name("query-popularity-flush").factory());
        flusher.scheduleWithFixedDelay(this::flush, 10, 10, java.util.concurrent.TimeUnit.SECONDS);
    }

    @Override
    public void recordObservation(QueryHash queryHash, String physicalSql) {
        synchronized (pending) {
            if (closed) return;
            PopularityObservation previous = pending.get(queryHash);
            if (previous == null && pending.size() >= MAX_PENDING) return;
            pending.put(queryHash, new PopularityObservation(physicalSql, previous == null ? 1 : previous.count() + 1));
        }
    }

    private void flush() {
        synchronized (flushLock) {
            java.util.Map<QueryHash, PopularityObservation> batch;
            synchronized (pending) {
                if (pending.isEmpty()) return;
                batch = new java.util.LinkedHashMap<>(pending);
                pending.clear();
            }
            try {
                RedisAsyncCommands<String, String> commands = connection.async();
                java.util.List<CompletableFuture<?>> writes = new java.util.ArrayList<>(batch.size() * 3);
                batch.forEach((hash, observation) -> {
                    String key = metadataKey(hash.value());
                    writes.add(commands.zincrby(CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY,
                            observation.count(), hash.value()).toCompletableFuture());
                    writes.add(commands.hset(key, PHYSICAL_SQL_FIELD, observation.sql()).toCompletableFuture());
                    writes.add(commands.expire(key, retention).toCompletableFuture());
                });
                CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new))
                        .get(5, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                restore(batch);
            } catch (Exception unavailable) {
                restore(batch);
                LOGGER.warning("popularity flush failed; bounded observations retained for retry: "
                        + unavailable.getClass().getSimpleName());
            }
        }
    }

    private void restore(java.util.Map<QueryHash, PopularityObservation> batch) {
        synchronized (pending) {
            batch.forEach((hash, old) -> {
                PopularityObservation current = pending.get(hash);
                if (current != null) pending.put(hash, new PopularityObservation(current.sql(), current.count() + old.count()));
                else if (pending.size() < MAX_PENDING) pending.put(hash, old);
            });
        }
    }

    @Override
    public long hitCount(QueryHash queryHash) {
        flush();
        return Optional.ofNullable(connection.sync().zscore(
                        CacheKeyConstants.QUERY_TRACKER_TOP_SORTED_SET_KEY, queryHash.value()))
                .map(Double::longValue)
                .orElse(0L);
    }

    @Override
    public List<PopularQuery> topByPopularity(int limit) {
        flush();
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
        closed = true;
        flusher.shutdown();
        flush();
        connection.close();
        redisClient.shutdown();
    }
}
