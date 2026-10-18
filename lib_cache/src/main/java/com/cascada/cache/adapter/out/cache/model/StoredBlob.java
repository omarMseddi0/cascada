package com.cascada.cache.adapter.out.cache.model;

import java.time.Instant;

public record StoredBlob(byte[] blob, Instant expiresAt) {
}
