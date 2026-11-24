package com.cascada.cache.application.service;

import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.QueryExecutorPort;
import com.cascada.cache.application.port.out.SqlCanonicalizerPort;
import com.cascada.cache.application.model.query.QueryResult;
import com.cascada.cache.application.service.cube.ExactCubeExecution;
import com.cascada.cache.domain.model.query.CanonicalQueryObject;
import com.cascada.cache.exception.UncacheableQueryException;
import java.util.Objects;
import java.util.Optional;

/** Translates logical SQL, canonicalizes it, and delegates the physical query to the cache use case. */
public final class ExecuteLogicalQueryService implements ExecuteLogicalQueryUseCase {

    private final LogicalSqlTranslatorPort translator;
    private final SqlCanonicalizerPort canonicalizer;
    private final ExecuteCachedQueryUseCase executeCachedQuery;
    private final QueryExecutorPort executor;
    private final ExactCubeExecution exactCube;

    public ExecuteLogicalQueryService(LogicalSqlTranslatorPort translator,
                                     SqlCanonicalizerPort canonicalizer,
                                     ExecuteCachedQueryUseCase executeCachedQuery,
                                     QueryExecutorPort executor) {
        this(translator, canonicalizer, executeCachedQuery, executor, null);
    }

    public ExecuteLogicalQueryService(LogicalSqlTranslatorPort translator,
                                     SqlCanonicalizerPort canonicalizer,
                                     ExecuteCachedQueryUseCase executeCachedQuery,
                                     QueryExecutorPort executor, ExactCubeExecution exactCube) {
        this.exactCube = exactCube;
        this.executor = Objects.requireNonNull(executor, "executor");
        this.translator = Objects.requireNonNull(translator, "translator");
        this.canonicalizer = Objects.requireNonNull(canonicalizer, "canonicalizer");
        this.executeCachedQuery = Objects.requireNonNull(executeCachedQuery, "executeCachedQuery");
    }

    @Override
    public QueryResult query(String logicalSql) {
        String physicalSql = translator.translateToPhysicalSql(logicalSql);
        var canonical = canonicalize(physicalSql);
        if (exactCube == null) return executeLegacy(physicalSql, canonical);
        if (canonical.isPresent()) {
            var directHit = executeCachedQuery.tryReadComplete(canonical.get());
            if (directHit.isPresent()) return directHit.get();
        }
        return exactCube.execute(physicalSql, () -> executeLegacy(physicalSql, canonical));
    }

    private Optional<CanonicalQueryObject> canonicalize(String physicalSql) {
        try {
            return Optional.of(canonicalizer.canonicalize(physicalSql));
        } catch (UncacheableQueryException unsupported) {
            return Optional.empty();
        }
    }

    private QueryResult executeLegacy(String physicalSql, Optional<CanonicalQueryObject> canonical) {
        return canonical.map(executeCachedQuery::execute)
                .orElseGet(() -> new QueryResult(executor.execute(physicalSql), false));
    }
}
