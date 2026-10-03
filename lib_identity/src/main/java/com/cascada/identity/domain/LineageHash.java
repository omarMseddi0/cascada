package com.cascada.identity.domain;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A deterministic hash over the set of source-table versions a cache entry depends on
 * (plan §8.4: {@code lineage_hash = sha256(sorted(source_table_versions))}).
 *
 * <p>When a Delta commit advances a source table's version, the recomputed lineage hash
 * differs, so the dependent entry's key no longer resolves — lineage-precise invalidation
 * without a separate flush step (plan §8.5).
 */
public record LineageHash(String value) {

    public LineageHash {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("lineage hash value must not be null or blank");
        }
    }

    public static LineageHash of(String value) {
        return new LineageHash(value);
    }

    /**
     * The empty lineage used by queries that do not yet declare source-table versions.
     * Distinct from any real hash so it never collides with a computed lineage.
     */
    public static LineageHash empty() {
        return new LineageHash("no-lineage");
    }

    /**
     * Builds the canonical, order-independent string that a hashing adapter signs to produce the
     * lineage hash. Duplicate source versions are removed, the remaining values are sorted, and each
     * value is encoded as its Java string length followed by {@code :} and the value. This format
     * replaces delimiter joining because source values may themselves contain the delimiter. The
     * resulting serialization is independent of source order and unambiguous when decoded.
     */
    public static String canonicalSourceVersionString(List<String> sourceTableVersions) {
        return sourceTableVersions.stream()
                .distinct()
                .sorted()
                .map(version -> version.length() + ":" + version)
                .collect(Collectors.joining());
    }
}
