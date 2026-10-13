package com.cascada.cache.adapter.out.cache;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisFuture;
import java.util.List;

record PendingMget(int startIndex, int keyCount,
                           RedisFuture<List<KeyValue<byte[], byte[]>>> future) { }
