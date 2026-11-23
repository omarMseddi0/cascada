package com.cascada.cache.fixture;

import com.cascada.cache.adapter.out.cache.ValkeyCacheBackendAdapter;
import com.cascada.cache.adapter.out.tracking.ValkeyQueryPopularityAdapter;
import com.cascada.cache.application.port.out.CacheValueSerializerPort;
import com.cascada.cache.domain.key.CacheKeyConstants;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.assertj.core.api.Assertions.assertThat;

/** Connects tests to real Valkey and cleans keys owned by the test. */
public final class ValkeyTestDatabase implements BeforeEachCallback, AfterEachCallback {

    public static final String ENVIRONMENT_VARIABLE = "CASCADA_TEST_VALKEY_URI";

    private final List<AutoCloseable> adapters = new ArrayList<>();
    private RedisClient client;
    private StatefulRedisConnection<String, String> connection;
    private boolean startedEmpty;

    public String uri() {
        return System.getenv(ENVIRONMENT_VARIABLE);
    }

    public ValkeyCacheBackendAdapter cache(CacheValueSerializerPort serializer) {
        var adapter = new ValkeyCacheBackendAdapter(uri(), serializer);
        adapters.add(adapter);
        return adapter;
    }

    public ValkeyQueryPopularityAdapter popularity() {
        var adapter = new ValkeyQueryPopularityAdapter(uri(), Duration.ofDays(7));
        adapters.add(adapter);
        return adapter;
    }

    public void expireBucket(String key) {
        assertThat(CacheKeyConstants.isBucketKey(key)).isTrue();
        assertThat(connection.sync().pexpireat(key, 1L)).as("bucket exists before expiration").isTrue();
    }

    public void clearCascadaKeys() {
        if (!startedEmpty) {
            throw new IllegalStateException("Valkey test database was not initialized");
        }
        var commands = connection.sync();
        var keys = new ArrayList<>(commands.keys(CacheKeyConstants.CACHE_KEY_PREFIX + ":*"));
        keys.addAll(commands.keys(CacheKeyConstants.QUERY_TRACKER_KEY_PREFIX + ":*"));
        if (!keys.isEmpty()) {
            commands.del(keys.toArray(String[]::new));
        }
        assertThat(commands.dbsize()).as("dedicated Valkey test database after cleanup").isZero();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        String redisUri = uri();
        if (redisUri == null || redisUri.isBlank()) return;
        try {
            client = RedisClient.create(redisUri);
            connection = client.connect();
            assertThat(connection.sync().dbsize()).as("dedicated Valkey test database starts empty").isZero();
            startedEmpty = true;
        } catch (RuntimeException | Error failure) {
            try {
                closeConnection();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        try {
            closeAdapters();
        } finally {
            try {
                if (startedEmpty) clearCascadaKeys();
            } finally {
                closeConnection();
            }
        }
    }

    private void closeAdapters() throws Exception {
        Exception firstFailure = null;
        for (var adapter : adapters.reversed()) {
            try {
                adapter.close();
            } catch (Exception failure) {
                if (firstFailure == null) firstFailure = failure;
                else firstFailure.addSuppressed(failure);
            }
        }
        adapters.clear();
        if (firstFailure != null) throw firstFailure;
    }

    private void closeConnection() {
        startedEmpty = false;
        try {
            if (connection != null) connection.close();
        } finally {
            connection = null;
            if (client != null) client.shutdown();
            client = null;
        }
    }
}
