package com.cascada.cache.application.service;

import com.cascada.cache.application.port.in.ExecuteCachedQueryUseCase;
import com.cascada.cache.application.port.in.ExecuteLogicalQueryUseCase;
import com.cascada.cache.application.port.out.LogicalSqlTranslatorPort;
import com.cascada.cache.application.port.out.SqlCanonicalizerPort;
import com.cascada.cache.domain.CanonicalQueryObject;

import java.util.Objects;

/**
 * The full read path for a query submitted as logical SQL:
 *
 * <pre>
 *   logical SQL ──▶ LogicalSqlTranslatorPort  (logical names → storage path + physical columns)
 *               ──▶ SqlCanonicalizerPort      (physical SQL → CanonicalQueryObject)
 *               ──▶ ExecuteCachedQueryUseCase (safety rules → cache path, or bypass to the executor)
 * </pre>
 *
 * <p><b>Why this class exists.</b> This three-step sequence previously lived in {@code CascadaEngine} in
 * the {@code app} module — that is, inside the composition root. A composition root is supposed to do
 * nothing but wire objects together; the moment it also decides <em>the order in which translation,
 * canonicalisation and execution happen</em>, that decision can only be tested by standing up the whole
 * application, and a second driving adapter (REST, JDBC, a scheduler) either duplicates the sequence or
 * reaches into the app module. Both are the symptom of business logic sitting one ring too far out.
 *
 * <p>Here, in the application layer behind {@link ExecuteLogicalQueryUseCase}, the sequence is testable
 * with three fakes and is shared by every driving adapter that will ever exist.
 *
 * <p>Note that translation and canonicalisation are <em>outbound</em> ports even though they happen at
 * the start of an inbound call. Direction of a port is not about when it runs — it is about who is in
 * control. The application calls out to them; they never call in.
 */
public final class ExecuteLogicalQueryService implements ExecuteLogicalQueryUseCase {

    private final LogicalSqlTranslatorPort translator;
    private final SqlCanonicalizerPort canonicalizer;
    private final ExecuteCachedQueryUseCase executeCachedQuery;

    public ExecuteLogicalQueryService(LogicalSqlTranslatorPort translator,
                                     SqlCanonicalizerPort canonicalizer,
                                     ExecuteCachedQueryUseCase executeCachedQuery) {
        this.translator = Objects.requireNonNull(translator, "translator");
        this.canonicalizer = Objects.requireNonNull(canonicalizer, "canonicalizer");
        this.executeCachedQuery = Objects.requireNonNull(executeCachedQuery, "executeCachedQuery");
    }

    @Override
    public ExecuteCachedQueryUseCase.Result query(String logicalSql) {
        String physicalSql = translator.translateToPhysicalSql(logicalSql);
        CanonicalQueryObject canonicalObject = canonicalizer.canonicalize(physicalSql);
        return executeCachedQuery.execute(canonicalObject);
    }
}
