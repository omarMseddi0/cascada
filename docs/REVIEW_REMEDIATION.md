# Review reconciliation and verified fixes

Reviewed on 13 October 2026 against the current working tree, starting from `ea85ec7`.
Sources: `review_claude.md` (11 October) and the sibling `CASCADA_REVIEW_AGAINST_PLAN.md`
(September review with subsequent audit additions).

The reports are evidence to investigate, not an implementation checklist. The September report
describes an older runtime; several of its claims no longer apply. Correctness defects, conditional
deployment risks, performance hypotheses, stylistic preferences, and missing roadmap features need
different treatment. This pass implements fixes to existing behavior only, without subagents.

## Verified and corrected

| Finding | Actual correction |
|---|---|
| Negated/symmetric BETWEEN treated as an ordinary time bound | A shared `TimeBoundClassifier` accepts only non-negated asymmetric BETWEEN with integral bounds. Other forms remain business predicates, or bypass caching when no safe window exists. |
| Gap queries drop fractional predicates retained in the cache key | Canonicalization and rewriting use the same classifier. Regression tests check both sides of the contract. |
| Inner subquery windows can be overwritten | Derived-table time scopes are rejected for cache/gap rewriting rather than guessed. Existing logical translation already rejects nested sources. |
| Reserved time-column names produce invalid gap SQL | Gap bounds are constructed as AST nodes; the unparser consults parser keyword metadata when quoting identifiers. Ordinary names still use minimal quoting. |
| The one-argument factory ignores configured time dimensions | Both factory entry points now respect the configured map. |
| Cube OR predicates are mistaken for one equality literal | Equality accepts complete literals, and IN-list narrowing validates the list. Unsupported disjunctions fall through to ordinary execution rather than returning an empty cached answer. Quoted strings containing OR remain supported. |
| ORDER BY silently ignores a missing result column | Resolve supported case/quoting differences; reject unresolved or ambiguous columns so the engine can execute the authoritative SQL instead of applying LIMIT to arbitrary rows. |
| Prefix flush can delete foreign keys and tracker state | Require the Cascada bucket namespace and match only well-formed bucket keys. HTTP/gRPC reject invalid prefixes before invoking the flush port. |
| One cancellation callback aborts the rest | Isolate and log callback failures during cancellation. Check cancellation before authoritative fallback and subsequent Spark work. |
| Valid cache payload corruption can pass structural checks | Enable Zstandard checksums in both compression paths. A payload-corruption regression verifies checksum failure; old non-checksummed blobs remain readable. |
| Worker Errors can leave HTTP/gRPC clients hanging | Return a terminal error/status before rethrowing fatal worker errors. HTTP logs include the cause; client responses omit internal details. |
| Manifest settings can inject YAML/JSON/XML structure | Validate substitution keys and simple scalar values before rendering. Reject structural syntax instead of interpolating it. Unicode table names and paths remain supported. This does not restrict an operator's choice of image or host mount. |
| Previously written buckets can retain incorrect predicate results | Bump canonical semantics version from 2 to 3 so buckets written under the older rewriting rules are not reused. Existing entries remain stored until ordinary flush/retention handling. |

## Findings that must not be applied blindly

- Large results are part of the engine's intended behavior. No returned-row cap, decoded-byte
  budget, request-concurrency gate, or new server-limit setting was added. A high row count alone
  does not prove a defect. Spark's existing result/resource settings remain operator-controlled.
- Typed column storage and aggregation already exist. The older claim that the active merger
  always builds row-map/list keys is stale for this working tree.
- Volatile/context-dependent query guards, exact cube numeric comparisons, schema checks,
  raw-time grouping, and nullable AVG fixes already exist. They were not rewritten as new work.
- Flush-all is already limited to Cascada bucket keys. The remaining defect was prefix flushing.
- Ordered projection signatures, alias preservation, and collision-free table replacement already
  address several September SQL findings. Java 17 and absent HTTP/gRPC/native-profile claims are
  also outdated for this working tree.
- Serial bucket jobs, cube locking, FIFO popularity eviction, bitmap allocation, and serializer
  buffering deserve workload-specific investigation. A source observation is not a measured
  performance regression, and a proposed replacement is not automatically faster or safer.
- Helper classes without a current caller are not automatically bugs. Do not delete public
  helpers or rewrite module boundaries simply to match a reviewer's preferred architecture.

## Real issues still open, or requiring a separate product decision

- **Source freshness:** keys and cube answers do not bind to a defined Delta snapshot. Correctness
  after source mutations requires one consistent snapshot contract across the original query,
  gaps, warming, and cache identity. Neither a TTL nor an event-time "today" cutoff alone proves
  that contract. No watcher, lineage API, or guessed freshness policy was added.
- **Deployment security:** anonymous listeners, TLS, administrative authorization, RBAC, and
  host-mount policy remain concerns in their actual deployment context. Authentication, token
  configuration, and admission-control additions were removed to respect the instruction not to
  implement new features. The manifest fix addresses structural injection only; it does not make
  the service production-security complete.
- **Memory and infrastructure trust:** the driver still materializes frames, and cache decoding
  still depends on available memory and trusted infrastructure. Integrity checks do not solve
  compression bombs or replace a chosen resource policy. No arbitrary result-size restriction was
  introduced under the guise of fixing large-data support.
- **Operational behavior:** partial-hit refill/coverage repair, source-change invalidation, flush
  races, popularity eviction, and cancellation of already-running sibling work need their own
  reproductions and focused changes. They are not claimed solved by this pass.
- **Roadmap features:** tenant routing, UI/SDKs, approximate-query integration, tiered storage,
  cost policies, new schedulers, and general SQL/table-format support were not implemented.

## Validation

JDK 21, direct Maven launcher, existing `validation,local-spark` profiles and required module opens:

```text
mvn -Pvalidation,local-spark -DforkCount=0 clean test
```

The clean full reactor passed: **645 cases reported by Maven, 619 passed, 26 skipped,
0 failures, 0 errors**. The skips are the existing opt-in native/Spark/Valkey/large-result tests.
After preserving Unicode manifest values, a focused Fabric reactor run also passed:
**35 Fabric tests and 23 Spark-configuration tests, 0 failures/errors**.
The final identifier-quoting follow-up passed **184 SQL tests**, along with Identity and Cache tests.
The final Cache check passed **290 Cache tests and 13 Identity tests** after preserving the existing
large-frame allocation behavior.

Logs: `target/review-full-tests.log`, `target/review-fabric-tests.log`, `target/review-sql-tests.log`,
and `target/review-cache-tests.log`.
No production deployment, external Valkey integration, or native benchmark was run.
The original review files and the existing commit history were preserved; these fixes are
uncommitted working-tree changes.
