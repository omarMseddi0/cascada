package com.cascada.cache.adapter.out.cache.model;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import java.util.List;

public record PendingMget(int startIndex, int keyCount,
                           RedisFuture<List<KeyValue<byte[], byte[]>>> future) { }
