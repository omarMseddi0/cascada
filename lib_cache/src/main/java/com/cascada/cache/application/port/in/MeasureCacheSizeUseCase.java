package com.cascada.cache.application.port.in;

import com.cascada.cache.domain.admin.CacheSizeReport;
import com.cascada.identity.domain.TenantIdentifier;

/**
 * <b>Primary (driving) port</b> — the administrator console's "how much is in cache?" button.
 *
 * <p>The figure is measured in <em>stored blob bytes</em> at the backend, never as a live JVM-heap
 * estimate, so the number an operator reads is the number they pay to store.
 */
public interface MeasureCacheSizeUseCase {

    /** The whole-cache report across every tenant. */
    CacheSizeReport measureCacheSize();

    /** One tenant's slice, computed from the same keyspace walk so the two figures reconcile. */
    CacheSizeReport measureCacheSize(TenantIdentifier tenant);
}
