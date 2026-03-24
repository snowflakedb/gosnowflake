# Design Document: Session State Detection for Dirty Session Detection in Connection Pools

---

## 1. Background and Motivation

### The Connection Pool Problem

Connection pools work by reusing established Snowflake sessions across multiple client requests. When a connection is borrowed from the pool, it has a known state: a specific database, schema, warehouse, role, and a set of session parameters. When that connection is returned to the pool, the pool assumes it can be safely lent to the next caller in the same state it was originally created.

This assumption breaks when user code modifies session state during the lifetime of a borrowed connection. If state changes are not detected, the next borrower silently inherits an unexpected environment — a different database context, modified query behavior, or an open transaction. This is the **dirty session problem**.

### Why This Matters

Snowflake sessions carry substantial per-session mutable state:

- **Execution context**: the active database, schema, warehouse, and role
- **Session parameters**: hundreds of parameters that affect query planning, output formatting, timezone handling, resource limits, and more
- **Transaction state**: whether a non-autocommit transaction is currently open

Any of these can be modified by user code — explicitly (e.g., `USE DATABASE`, `ALTER SESSION SET`) or implicitly (e.g., a `BEGIN` statement that is never committed). If the pool does not detect this, the next borrower receives a session in an unexpected state.

### Current Signals in Query Responses

Snowflake query responses already include several fields that partially address this:

| Signal | Covers |
|---|---|
| `finalDatabaseName`, `finalSchemaName`, `finalWarehouseName`, `finalRoleName` | Execution context changes |
| `parameters` array | Client-relevant parameter changes for ALTER/USE/SCL/UNKNOWN statement types |
| `statementTypeId` | Type of the top-level statement |
| `resultTypes` | Statement types of all sub-statements in a multi-statement batch |

---

## 2. What Makes a Session Dirty

A session is considered dirty and must not be returned to the connection pool if **any** of the following have changed since the connection was borrowed:

### 2.1 Execution Context Changes

| Change | Example Statement | Currently Detectable? |
|---|---|---|
| Active database changed | `USE DATABASE other_db` | Yes — `finalDatabaseName` |
| Active schema changed | `USE SCHEMA other_schema` | Yes — `finalSchemaName` |
| Active warehouse changed | `USE WAREHOUSE other_wh` | Yes — `finalWarehouseName` |
| Active role changed | `USE ROLE other_role` | Yes — `finalRoleName` |

### 2.2 Session Parameter Changes

| Change | Example Statement | Detectable via statement type? |
|---|---|---|
| Parameter changed (top-level) | `ALTER SESSION SET QUERY_TAG = 'x'` | Yes — `statementTypeId = ALTER_SESSION (0x4100)` |
| Parameter changed inside multi-statement | `SELECT 1; ALTER SESSION SET ...` | Yes — appears in `resultTypes` as `0x4100` |
| Parameter changed inside regular stored procedure | SP body calls `ALTER SESSION SET ...` | Irrelevant — SP runs in a **child session**; caller's session is unaffected |
| Parameter changed inside `EXECUTE IMMEDIATE` | `EXECUTE IMMEDIATE 'ALTER SESSION SET ...'` | **No** — outer type is opaque; inner SQL not reflected in `statementTypeId` |
| Parameter changed inside Snowflake Scripting block | `BEGIN ALTER SESSION SET ...; END` | **No** — outer type is opaque |

### 2.3 Transaction State

| Change | Example Statement | Detectable via statement type? |
|---|---|---|
| Non-autocommit transaction opened (top-level) | `BEGIN` | Yes — `statementTypeId = BEGIN_TRANSACTION (0x5400)` |
| Transaction opened inside multi-statement | `SELECT 1; BEGIN` | Yes — appears in `resultTypes` as `0x5400` |
| Transaction opened inside regular stored procedure | SP body calls `BEGIN` | Irrelevant — child session |
| Transaction opened inside `EXECUTE IMMEDIATE` | `EXECUTE IMMEDIATE 'BEGIN'` | **No** |

### 2.4 Temporary Objects (Session Variables, Temporary Tables, and More)

All temporary session-scoped objects in Snowflake — session variables, temporary tables, temporary stages, temporary file formats, temporary sequences, temporary functions, temporary secrets, and temporary Cortex agents — are persisted in GS as DPO entities in FDB, scoped to a session via `tempId = session.getId()`. They can all be included in the hash.

**FDB index**: `BaseDictionaryDPO` defines a `BYTEMP_ACTIVE_SLICE` index keyed by `(accountId, tempId, id)`. All temporary entity types are written to this index. Querying with `(accountId, sessionId)` returns only objects belonging to that session in **O(temp objects in session)** time — not an account-wide scan. This is the same index already used by `deleteActiveByTempIds()` for session cleanup, which each entity type calls:

```
Session.doPostCloseSession()
  → Table.dropAllTempTablesForSessions()
  → Stage.dropAllTempStagesForSessions()
  → FileFormat.dropAllTempFormatsForSessions()
  → Function.dropAllTempFunctionsForSessions()
  → SessionVariable.dropAllTempVariablesForSessions()
  → Secret.dropAllTempSecretsForSessions()
  → CortexAgent.dropAllTempAgentsForSessions()
```

**Gap**: There is no single unified cross-entity read method. Each DAO would need a `findActiveByTempId()` read counterpart to `deleteActiveByTempIds()`, and `SessionStateHashComputer` would call each one. Adding these methods to `BaseDictionaryDAO` is straightforward — the delete side already demonstrates the pattern.

**Why this matters for connection pools**: if a borrower creates a temporary table during a session, the hash changes at return time and the connection is correctly marked dirty — preventing the next borrower from inheriting unexpected temporary objects.

**Rollout**: temporary objects (beyond session variables) are gated behind a separate sub-flag from the main `ENABLE_SESSION_STATE_HASH` flag, so the per-query cost of querying multiple DAOs can be evaluated independently in production before enabling broadly.

---

## 3. Candidate Solutions

### Option A: Driver-Side Statement Type Tracking

**Approach**: The driver inspects `statementTypeId` on every top-level response and all entries in `resultTypes` for multi-statement batches. If any statement matches a known "dirtying" type (`ALTER_SESSION`, `USE_*`, `BEGIN_TRANSACTION`, `COMMIT`, `ROLLBACK`), the connection is marked dirty.

This approach is stronger than it initially appears:

- Regular stored procedures (`CALL`) run in a **child session** — `ALTER SESSION` or `BEGIN` inside a stored procedure does NOT affect the caller's session, so `CALL` itself does not need to be treated as a dirtying type
- Multi-statement batches expose all child statement types via `resultTypes` — an `ALTER SESSION` as a child of a multi-statement is correctly visible as `0x4100`

**Pros**:
- No backend changes required
- Works for top-level statements and multi-statement children immediately
- Zero latency overhead on the hot path
- Covers the vast majority of real-world use cases

**Cons**:
- Does not detect changes made inside `EXECUTE IMMEDIATE` or Snowflake Scripting blocks — the outer statement type is opaque to the driver
- Server-side-only parameter changes (`ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 1`) trigger `ALTER_SESSION` in `statementTypeId`, but the driver cannot distinguish client-relevant from server-only changes — both cause the connection to be marked dirty (acceptable: conservative is correct)
- Requires the driver to maintain an exhaustive list of "dirtying" statement types — fragile as new statement types are added to the server
- `parameters` array is only sent for ALTER/USE/SCL/UNKNOWN statement types for modern clients, not for `MULTI_STATEMENT` type parent jobs — parameter state is not directly readable for multi-statement batches

**Verdict**: Sufficient for common cases. The practical gap is `EXECUTE IMMEDIATE` and Snowflake Scripting blocks containing session-mutating statements. Reasonable pragmatic choice if those scenarios are acceptable gaps.

---

### Option B: `isSessionDirty` Boolean Flag

**Approach**: The server computes whether the session's state has changed and returns an `isSessionDirty` boolean in every query response.

**The fundamental problem**: the server does not know what the pool considers "clean." The server does not know when a connection was borrowed, so it has no baseline to compare against.

To make this work, the protocol would need an additional "set checkpoint" operation that the pool calls at borrow time, telling the server what state to treat as the clean baseline. This adds a new RPC, extra latency on every borrow, and more protocol surface.

If the baseline is instead defined as "state at login time," the approach breaks for pools that allow intentional state changes during connection warm-up, or that reset and reuse connections for different logical pools.

**The deeper insight**: `isSessionDirty` and `sessionStateHash` are computed from the same underlying data. The difference is not what is computed but **who holds the baseline**:

- With a hash: the driver records the hash at borrow time (it is the only party that knows when a borrow happened) and compares at return time
- With `isSessionDirty`: the server makes the judgment, but requires external context (the borrow time baseline) that only the driver has

A `isSessionDirty` boolean is architecturally equivalent to a hash with the comparison on the server side — but it requires the driver to communicate the baseline to the server first. The hash approach skips that round-trip by delegating the comparison to the driver, which already holds the needed context.

**Verdict**: Adds protocol complexity without benefit over the hash approach. The hash is the simpler realization of the same concept.

---

### Option C: `sessionStateVersion` Monotonic Counter

**Approach**: A monotonically increasing integer counter stored in `SessionDPO`, incremented every time the session's state is mutated. Returned in every response as `sessionStateVersion`. The pool records the version at borrow time and compares at return time.

**Pros**:
- Simple integer comparison at return time

**Cons**:
- Requires a new field in `SessionDPO` (FDB schema change, DPO version bump)
- Requires instrumenting every mutation site that modifies session state — `ExecAlterSession`, `ExecUse*`, transaction hooks — to increment the counter; any missed site is a silent bug
- FDB write on every state-modifying statement adds latency
- Does not cover `EXECUTE IMMEDIATE` unless the inner execution path also increments the counter (same gap as statement-type tracking, but harder to audit)
- Correctness depends on instrumentation completeness, not on the data itself — bugs produce false negatives with no observable indication

**Verdict**: More complex and fragile than the hash approach for no additional benefit. Correctness is instrumentation-dependent rather than data-derived.

---

### Option D: `sessionStateHash` — Chosen Solution

**Approach**: On every query response and login response, compute a hash of the current session state from live in-memory data and return it as `sessionStateHash`. The pool records the hash when the connection is borrowed and compares it when the connection is returned. A hash mismatch indicates the session is dirty.

This is the same concept as `isSessionDirty` but with the comparison correctly placed on the driver side, which is the only party that has the borrow-time baseline.

The hash input is:
```
H(
  defaultDatabaseName,
  defaultSchemaName,
  wantedWarehouseName,
  wantedRoleName,
  sorted[(parameterId, value) for all entries in PersistedParameters.configurationsMap],
  getCurrentTxnStartTime(),   // 0L (INVALID_TXN_ID) if no open non-autocommit transaction
  sorted[(name, encodedValue) for all SessionVariableDPO entries for this session],
  sorted[id for all temp TableDPO entries for this session],       // gated separately
  sorted[id for all temp StageDPO entries for this session],       // gated separately
  sorted[id for all temp FileFormatDPO entries for this session],  // gated separately
  // ... other temp entity types follow the same pattern
)
```

**Why these inputs**:

- `defaultDatabaseName`, `defaultSchemaName`, `wantedWarehouseName`, `wantedRoleName`: From `SessionDPO`; reflect current execution context
- `PersistedParameters.configurationsMap`: Contains **only parameters explicitly set via `ALTER SESSION SET`** — populated by `ConfigurationDAO.findAllSetParameters()`, which returns only `ConfigurationDPO` entities that exist for this session. Parameters at their default value have no DPO and are absent. A clean session has an empty map; a session with three overrides has three entries. This covers ALL altered parameters, including server-side-only ones never sent in the `parameters` array
- `getCurrentTxnStartTime()`: Already on the `ISession` interface; lazy-loads from FDB if not cached; returns `INVALID_TXN_ID (0L)` for no open transaction
- Session variables and temporary objects: Enumerated per entity type via a new `findActiveByTempId(accountId, sessionId)` method on `BaseDictionaryDAO`, using the existing `BYTEMP_ACTIVE_SLICE` index keyed by `(accountId, tempId, id)` — O(temp objects in session), not an account-wide scan. Temporary objects beyond session variables are gated behind a separate sub-flag — see §2.4

**Why this covers `EXECUTE IMMEDIATE` while statement-type tracking does not**: After an `EXECUTE IMMEDIATE 'ALTER SESSION SET ...'` completes, the session's `configurationsMap` has been mutated. The hash is computed from the actual map state, not from what statement was observed. The hash changes regardless of how the mutation was triggered.

**Pros**:
- No new FDB schema changes — all inputs are already in-memory at response time
- Correctness derives from the actual state data, not instrumentation — no mutation sites to miss
- Covers `EXECUTE IMMEDIATE` and Snowflake Scripting blocks that mutate session state
- Covers server-side-only parameter changes
- Correctly excludes changes inside regular stored procedures (child sessions; their `configurationsMap` is separate)
- Simple driver-side logic: record hash at borrow, compare at return
- No additional protocol round-trip needed at borrow time (unlike `isSessionDirty`)

**Cons**:
- Requires backend change to compute and return the hash
- Hash computation on every response adds a small CPU cost (bounded by number of set session parameters, typically small)
- Session variables (`SET myvar`) are stored in GS as `SessionVariableDPO` entities and will be included in the hash via a new `findActiveByTempId()` method on `BaseDictionaryDAO` — see §2.4
- Hash collisions are theoretically possible (negligible probability with 64-bit or 128-bit hash)

**Verdict**: Best available solution. Provides comprehensive coverage including `EXECUTE IMMEDIATE` and scripting blocks, with correctness guaranteed by the data itself rather than by instrumentation completeness. The driver-side comparison is architecturally correct since only the driver holds the borrow-time baseline.

---

### Solution Comparison

| Criterion | Statement Type Tracking | `isSessionDirty` | `sessionStateVersion` | `sessionStateHash` |
|---|---|---|---|---|
| Top-level dirtying statements | Yes | Yes | Yes | Yes |
| Multi-statement children | Yes (`resultTypes`) | Yes | Yes | Yes |
| Regular stored procedures | N/A (child session) | N/A | N/A | N/A |
| `EXECUTE IMMEDIATE` / Scripting | **No** | Yes | Yes | **Yes** |
| Server-side-only param changes | Yes (causes dirty) | Yes | Yes | Yes |
| FDB schema changes required | No | No | **Yes** | No |
| Backend changes required | No | Yes | Yes | Yes |
| Mutation sites to instrument | None | None | **Many** | None |
| Borrow-time checkpoint RPC needed | No | **Yes** | No | No |
| Correctness guarantee | Heuristic | Strong (with checkpoint) | Fragile | **Strong** |

---

## 4. Implementation Plan

### 4.1 Backend: GlobalServices

#### Phase 1: Session State Hash Computation Utility

Create a utility class `SessionStateHashComputer` that computes the hash from a live session object.

**Prerequisites**: Add `findActiveByTempId(long accountId, long sessionId)` to `BaseDictionaryDAO`, using `visitDposByRange()` on the existing `BYTEMP_ACTIVE_SLICE` index (keyed by `(accountId, tempId, id)`). The deletion counterpart `deleteActiveByTempIds()` already uses this slice, confirming the access pattern is supported.

**Inputs from `ISession`**:
- `session.getDefaultDatabaseName()`
- `session.getDefaultSchemaName()`
- `session.getWantedWarehouseName()`
- `session.getWantedRoleName()`
- `session.getParameters().getConfigurationsMap()` — sort by `parameter.getId()` before hashing
- `session.getCurrentTxnStartTime()` — already on `ISession`; returns `TransactionConstants.INVALID_TXN_ID (0L)` if no open non-autocommit transaction
- Session variables via `SessionVariable.findActiveByTempId(accountId, session.getId())` — sort by name before hashing

**Hash algorithm**: Use a stable, fast hash (e.g., Guava `Hashing.murmur3_128()`). Output as a hex string for JSON serialization.

```java
public static String computeSessionStateHash(ISession session) {
  Hasher hasher = Hashing.murmur3_128().newHasher();
  hashNullableString(hasher, session.getDefaultDatabaseName());
  hashNullableString(hasher, session.getDefaultSchemaName());
  hashNullableString(hasher, session.getWantedWarehouseName());
  hashNullableString(hasher, session.getWantedRoleName());

  session.getParameters().getConfigurationsMap().entrySet().stream()
      .sorted(Comparator.comparingInt(e -> e.getKey().getId()))
      .forEach(e -> {
        hasher.putInt(e.getKey().getId());
        hashNullableString(hasher, e.getValue().getValueAsString());
      });

  hasher.putLong(session.getCurrentTxnStartTime());

  SessionVariable.findActiveByTempId(session.getAccountId(), session.getId()).stream()
      .sorted(Comparator.comparing(SessionVariable::getName))
      .forEach(v -> {
        hashNullableString(hasher, v.getName());
        hashNullableString(hasher, v.getValueEncoded());
      });

  return hasher.hash().toString();
}
```

#### Phase 2: Add `sessionStateHash` to Query Responses

**File**: `SnowflakeMessageWriter.java`, in `writeExecutionContext()` (~line 940), after writing the `final*Name` fields:

```java
if (featureGate.isEnabled(Feature.SESSION_STATE_HASH)) {
  String hash = SessionStateHashComputer.computeSessionStateHash(session);
  out.key("sessionStateHash").value(hash);
}
```

#### Phase 3: Add `sessionStateHash` to Login Response

In the login/session creation response writer (likely `SessionResource` or the authentication handler), compute and include `sessionStateHash`. This gives the pool the initial baseline at login time without requiring a dummy query.

#### Phase 4: Feature Flag

Gate both Phase 2 and Phase 3 behind a server-side feature flag (e.g., `ENABLE_SESSION_STATE_HASH`). This allows gradual rollout, per-account validation, and rollback without redeployment.

---

### 4.2 Driver (All Drivers)

The following changes apply uniformly to all Snowflake drivers (Go, Python, JDBC, .NET, ODBC, PHP/PDO).

#### Phase 1: Parse `sessionStateHash`

Read `sessionStateHash` from:
- Login response body
- Every query response body

Store as a field on the connection object. As confirmed in Section 5, all drivers use lenient JSON parsing and will silently ignore the field on server versions that do not yet return it.

#### Phase 2: Record Hash at Borrow Time

When a connection is borrowed from the pool, record the current `sessionStateHash` as the clean baseline (`cleanStateHash`).

#### Phase 3: Compare Hash at Return Time

When a connection is returned to the pool, compare the `sessionStateHash` from the last response against `cleanStateHash`. If they differ, the session is dirty — do not return it to the pool, close it, and let the pool create a fresh connection.

Pseudocode applicable to all drivers:

```
function isDirty(connection):
    return connection.currentStateHash != connection.cleanStateHash
```

#### Phase 4: Connection Parameter

Expose a connection parameter (e.g., `dirtySessionDetection=hash|disabled`) in each driver.

---

### 4.3 Protocol Changes Summary

| Field | Added To | Type | Notes |
|---|---|---|---|
| `sessionStateHash` | Login response | `string` | Baseline hash at login time |
| `sessionStateHash` | Every query response | `string` | Current session state hash after execution |

No FDB schema changes. No new `SessionDPO` fields. No new RPC endpoints.

---

### 4.4 Testing Plan

| Test Scenario | Expected Behavior |
|---|---|
| Clean session — no state changes | Hash at return == hash at borrow |
| `USE DATABASE` executed | Hash changes |
| `ALTER SESSION SET` client-visible param | Hash changes |
| `ALTER SESSION SET` server-only param | Hash changes (key test — validates over statement-type tracking alone) |
| `BEGIN` (non-autocommit) | Hash changes (txnStartTime != 0) |
| `COMMIT` after `BEGIN` | Hash returns to pre-BEGIN value |
| Multi-statement: `SELECT 1; ALTER SESSION SET ...` | Hash changes after parent query completes |
| `EXECUTE IMMEDIATE 'ALTER SESSION SET ...'` | Hash changes (key test — validates over statement-type tracking) |
| Regular stored procedure changes own (child) session | Hash does NOT change (child session is isolated) |


---

## 5. Driver Compatibility

Adding `sessionStateHash` to login and query responses is **backward-compatible across all Snowflake drivers, including versions from at least 3 years ago**. No driver uses strict schema validation that would reject unknown response fields. This requires no coordination with driver teams and no version gating on the server side.

### 5.1 Per-Driver Analysis

| Driver | Language | JSON Library | Unknown Fields | Strict Mode Risk |
|---|---|---|---|---|
| gosnowflake | Go | `encoding/json` (stdlib) | Silently ignored | None |
| snowflake-connector-python | Python | `json.loads()` / requests | Silently ignored | None |
| snowflake-jdbc | Java | Jackson `JsonNode.path()` | Silently ignored | None |
| snowflake-connector-net | C# | Newtonsoft.Json | Silently ignored | None |
| snowflake-odbc | C++ | PicoJSON | Silently ignored | None |
| libsnowflakeclient | C | cJSON | Silently ignored | None |
| pdo_snowflake | PHP | cJSON (via libsnowflakeclient) | Silently ignored | None |

### 5.2 Mechanism per Driver

**gosnowflake** (`auth.go`, `query.go`): Uses `json.NewDecoder().Decode()` with Go's default behavior. Go's `encoding/json` silently discards unknown fields unless `DisallowUnknownFields()` is explicitly set on the decoder — it is not set anywhere in the codebase. Response structs use `json:"field,omitempty"` tags; extra fields from the server are simply ignored at the struct level.

**snowflake-connector-python** (`auth/_auth.py`, `network.py`): Parses all responses with `raw_ret.json()` (requests library → `json.loads()`). Field access uses `.get()` throughout, which returns `None` for unknown or missing keys. No marshmallow, no pydantic, no schema validation layer of any kind.

**snowflake-jdbc** (`SessionUtil.java`, `SFStatement.java`): Uses `ObjectMapper.readTree()` returning a raw `JsonNode`, then extracts fields manually with `.path("fieldName").asText()`. This approach is inherently lenient — `.path()` returns a `MissingNode` (not an error) for any field not present. `ObjectMapperFactory` does not set `FAIL_ON_UNKNOWN_PROPERTIES`. The only class using `@JsonIgnoreProperties` is `TokenResponseDTO` (OAuth), and it explicitly sets `ignoreUnknown = true`.

**snowflake-connector-net** (`RestResponse.cs`, `RestRequester.cs`, `JsonUtils.cs`): Newtonsoft.Json's default behavior ignores unknown fields. `JsonUtils.JsonSettings` explicitly does not set `MissingMemberHandling.Error`. All response model properties use `[JsonProperty(NullValueHandling = NullValueHandling.Ignore)]`, which only controls null value serialization — not unknown field rejection.

**snowflake-odbc** (`Connection.cpp`, `ResultSet.cpp`): Uses PicoJSON. The `getd<T>(defaultValue)` helper returns the default if a key is missing or null; `snowflake_cJSON_GetObjectItem()` returns null for fields not present. Extra fields in the response object are never iterated over — only explicitly requested fields are read.

**libsnowflakeclient** (`connection.c`, `client.c`): Uses cJSON with `snowflake_cJSON_GetObjectItem()`. Unknown fields are never accessed and never produce errors. The JSON helper functions (`json_copy_string`, `json_copy_bool`, etc.) only report `SF_JSON_ERROR_ITEM_MISSING` for fields they explicitly look for — they have no concept of unexpected fields. **pdo_snowflake** delegates 100% of C-level response parsing to libsnowflakeclient and adds no PHP-layer schema validation.

### 5.3 Historical Versions

The lenient parsing approach is either enforced by the language/library default or by deliberate architectural choice in every driver:

- **Go**: `encoding/json` has silently ignored unknown fields since Go 1.0.
- **Python**: `json.loads()` and dict `.get()` have never had a strict mode.
- **Java (JDBC)**: Manual `JsonNode.path()` extraction is architectural — it cannot fail on extra fields. Even annotated Jackson POJOs are lenient by default unless `ignoreUnknown = false` is explicitly set.
- **C# (.NET)**: Newtonsoft.Json ignores unknown fields by default; no version of the connector has ever set `MissingMemberHandling.Error`.
- **C/C++ (ODBC, libsnowflakeclient, pdo_snowflake)**: cJSON and PicoJSON both require explicit field extraction; the concept of "unexpected field validation" does not exist in these libraries.

In all cases, a 3-year-old version of any of these drivers will accept responses with additional fields without error.

---

## 6. Required Team Reviews

The implementation touches code owned by four distinct teams outside the authoring team (Client Backend). Each team's involvement is determined by the files they own in CODEOWNERS.

### 6.1 DBSec IAM — Authentication (`@snowflake-eng/dbsec-iam-gatekeepers`, `@snowflake-eng/dbsec-iam-authentication-gatekeepers`)

**Why**: This team owns `Session.java`, the full `GlobalServices/modules/dbsec/session/**` module, and the login/authentication resource layer. The implementation reads `getCurrentTxnStartTime()` from `Session.java` and writes `sessionStateHash` into the login response — both are squarely in their domain.

**Files that require their review**:
- `GlobalServices/src/main/java/com/snowflake/metadata/runtime/Session.java`
- `GlobalServices/modules/dbsec/session/**`
- Any login response changes under `GlobalServices/modules/dbsec/authn-impl/**`

**Key questions for this team**:
- Is `getCurrentTxnStartTime()` safe to call on every query response without performance concerns (it lazy-loads from FDB on first call)?
- Are there session lifecycle scenarios (session hijacking recovery, session forking, token renewal) where the hash could produce a false positive?

---

### 6.2 Database Security (`@snowflake-eng/database-security`)

**Why**: This team owns the broad `GlobalServices/src/main/java/com/snowflake/resources/**` package. The login response writer lives under this package, so any change to write `sessionStateHash` into the login response requires their review.

**Files that require their review**:
- The login response resource (e.g., `SessionResource.java` or equivalent under `resources/`)

**Key questions for this team**:
- Does adding a new field to the login response require any security review or audit trail changes?
- Does `sessionStateHash` constitute a fingerprint that could be used in an attack (e.g., oracle for probing session state)? The hash is a one-way function with no ability to reconstruct parameters from it, so the risk is low — but this team should validate.

---

### 6.3 Service Parameter Management (`@snowflake-eng/service-parameter-management`)

**Why**: This team owns `PersistedParameterTools.java`, the `sql/common/options/**` package (where `PersistedParameters` and `configurationsMap` live), and the feature flag / parameter infrastructure. The hash is computed over `configurationsMap`, and the rollout gate itself is a new server parameter.

**Files that require their review**:
- `GlobalServices/src/main/java/com/snowflake/metadata/configuration/PersistedParameterTools.java`
- `GlobalServices/src/main/java/com/snowflake/sql/common/options/**` (PersistedParameters)
- `GlobalServices/conf/parameters.yaml` (new feature flag parameter definition)
- `GlobalServices/modules/parameters/**` (parameter registration)

**Key questions for this team**:
- Is it safe to iterate `configurationsMap` on the hot query response path? What is the typical size in production (number of explicitly set session parameters)?
- Is there a preferred way to sort/serialize parameter values for stable hashing, given that `Configuration` may represent values differently for different parameter types?
- What is the correct parameter category and default rollout policy for the new `ENABLE_SESSION_STATE_HASH` feature flag?

---

### 6.4 Go Driver / Client (`@snowflakedb/Client`)

**Why**: The CODEOWNERS for `gosnowflake` assigns `@snowflakedb/Client` to all files. The driver-side implementation (parsing `sessionStateHash`, recording at borrow time, comparing at return time) lives entirely in their codebase.

**Files that require their review**:
- `auth.go` (login response parsing)
- `query.go` / `restful.go` (query response parsing)
- Connection pool integration (new `cleanStateHash` / `currentStateHash` fields and `isDirty()` logic)

**Key questions for this team**:
- Is the connection pool API the right integration point, or does this belong in a higher-level abstraction?
- What is the plan for other drivers (Python, JDBC, .NET, ODBC)? The Go driver is the primary target but the backend change benefits all drivers equally.

---

### 6.5 Summary Table

| Team | GitHub Handle | Role in This Change | Required? |
|---|---|---|---|
| Client Backend *(authoring team)* | `@snowflake-eng/client-backend-gate-keepers` | Owns `SnowflakeMessageWriter.java` (query response write path) | Internal |
| DBSec IAM — Authentication | `@snowflake-eng/dbsec-iam-gatekeepers` | Owns `Session.java`, session module, login response auth layer | **Yes** |
| Database Security | `@snowflake-eng/database-security` | Owns `resources/` package (login response resource) | **Yes** |
| Service Parameter Management | `@snowflake-eng/service-parameter-management` | Owns `PersistedParameters`, feature flag infrastructure | **Yes** |
| Go Driver / Client | `@snowflakedb/Client` | Owns gosnowflake — all driver-side implementation | **Yes** |
