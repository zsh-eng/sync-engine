# RFC-0002: Architecture for a Simplified Sync Engine `microsync`

## 1. Summary

This RFC proposes a simpler sync architecture with a narrow scope and opinionated defaults.
The design goal is to keep core types in one file and keep each implementation component small.

## 2. Problem

We need to handle:

- Multiple devices per user
- Offline edits and reconnect
- Conflict handling
- Serverless deployment constraints

The focus is personal/user-owned data with relatively low write frequency.

## 3. Proposed Architecture

### 3.1 Bag Of Rows

All synced data is represented as rows in a common envelope:

- `userId` (optional on client; required on multi-tenant server partitions)
- `namespace`
- `collectionId`
- `id`
- `parentId` (optional relation key)
- `data` (JSON value)
- `tombstone`
- `txId` (optional)
- `schemaVersion` (optional)
- `committedTimestampMs`
- `hlcTimestampMs`
- `hlcCounter`
- `hlcDeviceId`

Canonical identity key for row state: `(userId?, namespace, collectionId, id)`.

`parentId` is for one-to-many relationships and targeted pulls.

### 3.2 Storage

`Storage` exposes ergonomic methods for app code (no operation-object API required):

Read methods:

- `get(collectionId, id)`
- `getAll(collectionId)`
- `getAllWithParent(collectionId, parentId)`

Write methods:

- `put(collectionId, id, data, parentId?)`
- `delete(collectionId, id)`
- `deleteAllWithParent(collectionId, parentId)`
- `batchLocal(operations)` where operations are only atomic CUD operations (`put` and `delete`)

Remote apply:

- `applyRemote(rows)` for already-HLC-stamped rows from the server

KV API for metadata:

- `putKV`
- `getKV`
- `deleteKV`

Pending API:

- `getPending`
- `removePendingThrough`

Semantics:

- Any mutating method (`put`, `delete`, `deleteAllWithParent`, `batchLocal`) runs as a local-atomic write pipeline:
  1. Resolve concrete target rows (if needed)
  2. Allocate HLC batch
  3. Build canonical row envelopes (tombstones for deletes)
  4. Apply through adapter conflict resolution
  5. Append successful local changes to pending log
  6. Emit invalidation hints
- Read methods are side-effect free and do not allocate HLC values.
- KV persists sync metadata such as pull cursor and retry state.

### 3.3 Storage Adapter

`Storage` maps method calls to a narrow adapter core.
Adapters own conflict resolution and bulk execution details.

Recommended adapter primitives:

- `query({ collectionId, id?, parentId?, includeTombstones? })`
- `applyRows(rows)` (bulk LWW apply)
- `appendPending(operations)`, `getPending(limit)`, `removePendingThrough(sequenceInclusive)`
- `putKV`, `getKV`, `deleteKV` (or equivalent durable metadata store)

Notes:

- `get`, `getAll`, `getAllWithParent`, and internal read planning are all specializations of one `query` shape.
- `deleteAllWithParent` is implemented in `Storage` by querying matching live rows, stamping tombstones with fresh HLC, and sending atomic row deletes through `batchLocal`/`applyRows` (optionally chunked).

Examples:

- SQLite/D1 adapters can use set-based SQL (`ON CONFLICT ... WHERE ...`) for LWW.
- IndexedDB/Dexie adapters can `bulkGet` + in-memory resolution + `bulkPut` in one transaction.

Adapters also manage:

- Row table/schema/indexes
- Pending operation log table/schema/indexes
- Tombstone representation

### 3.4 Pending Operations

Local writes are appended to a pending operation log with a strictly increasing `sequence` key.

- Sequence source: auto-increment integer primary key
- Push order: ascending by sequence
- Ack/removal semantics: remove through sequence `N` (`<= N`)
- Row apply and pending append should be committed in one local transaction when backend capabilities allow.

This avoids ambiguity when many mutations target the same row.

### 3.5 Hybrid Logical Clock (HLC)

HLC provides deterministic LWW conflict ordering across devices.
Tie-break is `hlcDeviceId`.

### 3.6 Commit Timestamp And Cursor

Server commits are ordered by `(committedTimestampMs, collectionId, id)`.
The pull cursor is server-issued and represented by these fields.

`pull` accepts:

- `cursor?` (absent for first sync)
- `limit`
- optional `collectionId` and `parentId` filters

`pull` returns:

- `changes`
- `nextCursor`
- `hasMore`

### 3.7 Client And Server Symmetry

Client and server both store row envelopes and apply the same LWW/HLC rules.
Any node with row state can serve ordered pulls by commit timestamp.

Changing servers is equivalent to a first-time sync: clear local cursor and pull from the new server.

## 4. Connection Manager

`ConnectionManager` is a small FSM (for example: `offline`, `connected`, `needsAuth`, `paused`) driven by a platform-specific `ConnectionDriver`.

The sync loop subscribes to connection state and decides when to run.

## 5. Transport

`TransportAdapter` handles sync protocol IO:

- `pull({ cursor?, limit, collectionId?, parentId? })`
- `push({ operations })`
- `onEvent(listener)`

Auth configuration belongs to transport construction (cookie or bearer token mode).

## 6. Sync Loop

`SyncEngine` logic:

1. Load pending ops from storage in sequence order.
2. `push` pending ops.
3. On ack, `removePendingThrough(ackSequence)`.
4. Read cursor from KV.
5. `pull` with cursor and limit.
6. Apply pulled changes through `applyRemote` (adapter LWW).
7. Persist `nextCursor`.
8. Repeat while `hasMore`.

## 7. Type System Direction

TypeScript generics should be driven by app collection schemas.
Expected shape:

- `Collections` map keyed by `collectionId`
- method input type constrained by collection key
- method result inferred from collection key and method type
- `batchLocal` operation union is limited to `put` and `delete`
- support for Zod-based apps by passing inferred collection value types

This keeps API calls strongly typed while preserving a small runtime surface.

## 8. Known Trade-Offs

### 8.1 Clock Skew

We accept small skew between devices/servers for this product scope.
Large backward server clock movement can cause missing ranges when using timestamp-based cursors.
Given personal-app usage and low concurrency, this is an acceptable v1 trade-off.

### 8.2 Scope

This RFC optimizes for simple personal-data sync and not high-concurrency collaborative editing.

## 9. Open Questions

1. Should `schemaVersion` be required or optional in v1 row envelopes?
2. Should server pulls exclude rows authored by the requesting `hlcDeviceId`, or always return full ordered state?
3. Which index baseline should be mandatory for every server adapter beyond `(userId, namespace, committedTimestampMs, collectionId, id)`?
4. For collections with a required parent relationship, should `parentId` become mandatory on `put` to avoid read-before-write parent lookups?
