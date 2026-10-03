package com.cascada.cache.application.port.out;

import com.cascada.cache.domain.admin.CacheScope;
import com.cascada.cache.domain.admin.CacheSizeReport;

/** Storage operations used by cache administration and operations tooling. */
public interface CacheAdministrationPort {

    /** Measure stored bytes and bucket count across the cache. */
    CacheSizeReport sizeReport();

    /** Remove every bucket in the requested scope and return the number removed. */
    long flush(CacheScope scope);
}
