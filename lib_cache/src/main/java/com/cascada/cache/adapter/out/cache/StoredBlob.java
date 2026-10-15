package com.cascada.cache.adapter.out.cache;

import java.time.Instant;

record StoredBlob(byte[] blob, Instant expiresAt) {
}
