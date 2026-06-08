package com.cascada.cache.domain;

import com.cascada.identity.domain.LineageHash;
import com.cascada.identity.domain.PolicyVersion;
import com.cascada.identity.domain.QueryHash;
import com.cascada.identity.domain.SchemaVersion;
import com.cascada.identity.domain.TenantIdentifier;

/**
 * Builds cache keys. Two responsibilities:
 *
 * <ol>
 *   <li>the bucket key wire format, ported verbatim from {@code build_cache_key} in
 *       {@code domain.py}: {@code QC:V4:B<bucketSeconds>:<queryHash>:<bucketStartTs>}; and</li>
 *   <li>the tenant-scoped composition string the plan defines in §8.4,
 *       {@code tenant || logicHash || schemaVersion || lineageHash || policyVersion}, which a
 *       hashing adapter signs to produce the final isolated key — cross-tenant reads are
 *       impossible by construction because the tenant segment is inside the signed material.</li>
 * </ol>
 */
public final class CacheKeyFactory {

    private CacheKeyFactory() {
    }

    /** Ported from {@code build_cache_key(query_hash, bucket_start_ts, bucket_seconds)}. */
    public static String buildBucketKey(QueryHash queryHash, long bucketStartTimestampSeconds, long bucketSeconds) {
        return CacheConstants.CACHE_KEY_PREFIX
                + ":B" + bucketSeconds
                + ":" + queryHash.value()
                + ":" + bucketStartTimestampSeconds;
    }

    /**
     * The pre-image string the cache hashes to obtain a tenant-isolated, lineage-precise key
     * (plan §8.4). Distinct tenants, schema versions, lineage hashes, or policy versions yield a
     * different string, so they can never resolve to the same stored entry.
     */
    public static String buildTenantScopedKeyMaterial(TenantIdentifier tenant, QueryHash logicHash,
                                                       SchemaVersion schemaVersion, LineageHash lineageHash,
                                                       PolicyVersion policyVersion) {
        return tenant.asKeyPrefixSegment()
                + "||" + logicHash.value()
                + "||" + schemaVersion.value()
                + "||" + lineageHash.value()
                + "||" + policyVersion.value();
    }
}
