import type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  PendingSequence,
  RowQuery,
  RowApplyOutcome,
  RowStorageAdapter,
  StoredRow,
} from "../../core/types";
import { createSerialQueue } from "../../core/internal/serial-queue";
import {
  appendPendingOperations,
  getPendingOperations,
  removePendingOperationsThrough,
  rowKey,
} from "../../core/storage/shared";

const DEFAULT_ROWS_TABLE = "rows";
const DEFAULT_KV_TABLE = "sync_kv";
const ROW_BIND_PARAMETER_COUNT = 12;
const DEFAULT_MAX_ROWS_PER_STATEMENT = 90;

function assertNamespace(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Invalid namespace: expected a non-empty string");
  }

  return value;
}

function assertUserID(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Invalid userID: expected a non-empty string");
  }

  return value;
}

function assertSqlIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }

  return value;
}

function assertFiniteInteger(value: number, field: string): void {
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    throw new Error(`Invalid ${field}: expected non-negative integer, received ${value}`);
  }
}

function parseValueJson<Value>(valueJson: string): Value {
  try {
    return JSON.parse(valueJson) as Value;
  } catch (error) {
    throw new Error(`Invalid row value JSON: ${String(error)}`);
  }
}

function toValueJson(value: unknown): string {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) {
    throw new Error("Row value must be JSON-serializable");
  }
  return encoded;
}

export interface SqliteRowRecord {
  user_id: string;
  namespace: string;
  collection: string;
  id: string;
  parent_id: string | null;
  value_json: string | null;
  committed_timestamp_ms: number;
  hlc_wall_ms: number;
  hlc_counter: number;
  hlc_node_id: string;
  tx_id: string | null;
  tombstone: 0 | 1;
}

export interface SqliteStatementExecutor {
  run(sql: string, params?: ReadonlyArray<unknown>): Promise<void>;
  get<Row = unknown>(sql: string, params?: ReadonlyArray<unknown>): Promise<Row | undefined>;
  all<Row = unknown>(sql: string, params?: ReadonlyArray<unknown>): Promise<Row[]>;
}

export interface SqliteTransactionExecutor {
  transaction<Result>(
    runner: (executor: SqliteStatementExecutor) => Promise<Result>,
  ): Promise<Result>;
}

interface CreateSqliteRowStoreAdapterInput {
  executor: SqliteTransactionExecutor;
  userID: string;
  namespace: string;
  rowsTable?: string;
  kvTable?: string;
  maxRowsPerStatement?: number;
}

interface EncodedSqliteRow {
  collectionId: string;
  id: string;
  parentId: string | null;
  valueJson: string | null;
  committedTimestampMs: number;
  hlcTimestampMs: number;
  hlcCounter: number;
  hlcDeviceId: string;
  txId: string | null;
  tombstone: 0 | 1;
}

interface UpsertReturningRow {
  collection: string;
  id: string;
  parent_id: string | null;
  committed_timestamp_ms: number;
  hlc_wall_ms: number;
  hlc_counter: number;
  hlc_node_id: string;
  tombstone: 0 | 1;
}

function toStoredRow<S extends CollectionValueMap>(row: SqliteRowRecord): AnyStoredRow<S> {
  const committedTimestampMs = Number(row.committed_timestamp_ms);
  const hlcTimestampMs = Number(row.hlc_wall_ms);
  const hlcCounter = Number(row.hlc_counter);

  assertFiniteInteger(committedTimestampMs, "committed_timestamp_ms");
  assertFiniteInteger(hlcTimestampMs, "hlc_wall_ms");
  assertFiniteInteger(hlcCounter, "hlc_counter");

  return {
    namespace: row.namespace,
    collectionId: row.collection as CollectionId<S>,
    id: row.id,
    parentId: row.parent_id,
    data: row.value_json === null ? null : parseValueJson(row.value_json),
    tombstone: row.tombstone === 1,
    txId: row.tx_id ?? undefined,
    committedTimestampMs,
    hlcTimestampMs,
    hlcCounter,
    hlcDeviceId: row.hlc_node_id,
  };
}

function toEncodedSqliteRow<S extends CollectionValueMap>(row: AnyStoredRow<S>): EncodedSqliteRow {
  return {
    collectionId: row.collectionId,
    id: row.id,
    parentId: row.parentId,
    valueJson: row.data === null ? null : toValueJson(row.data),
    committedTimestampMs: row.committedTimestampMs,
    hlcTimestampMs: row.hlcTimestampMs,
    hlcCounter: row.hlcCounter,
    hlcDeviceId: row.hlcDeviceId,
    txId: row.txId ?? null,
    tombstone: row.tombstone ? 1 : 0,
  };
}

function createRowsSchemaStatements(rowsTable: string): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${rowsTable} (
      user_id TEXT NOT NULL,
      namespace TEXT NOT NULL,
      collection TEXT NOT NULL,
      id TEXT NOT NULL,
      parent_id TEXT,
      value_json TEXT,
      committed_timestamp_ms INTEGER NOT NULL,
      hlc_wall_ms INTEGER NOT NULL,
      hlc_counter INTEGER NOT NULL,
      hlc_node_id TEXT NOT NULL,
      tx_id TEXT,
      tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
      PRIMARY KEY (user_id, namespace, collection, id)
    )`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_idx ON ${rowsTable} (user_id, namespace)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_collection_idx ON ${rowsTable} (user_id, namespace, collection)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_collection_parent_idx ON ${rowsTable} (user_id, namespace, collection, parent_id)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_collection_tombstone_idx ON ${rowsTable} (user_id, namespace, collection, tombstone)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_committed_idx ON ${rowsTable} (user_id, namespace, committed_timestamp_ms, collection, id)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_user_namespace_hlc_idx ON ${rowsTable} (user_id, namespace, hlc_wall_ms, hlc_counter, hlc_node_id)`,
  ];
}

function createKVSchemaStatements(kvTable: string): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${kvTable} (
      user_id TEXT NOT NULL,
      namespace TEXT NOT NULL,
      key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      PRIMARY KEY (user_id, namespace, key)
    )`,
  ];
}

function createSchemaStatements(rowsTable: string, kvTable: string): string[] {
  return [...createRowsSchemaStatements(rowsTable), ...createKVSchemaStatements(kvTable)];
}

export function createSqliteRowStoreSchemaStatements(
  rowsTable = DEFAULT_ROWS_TABLE,
  kvTable = DEFAULT_KV_TABLE,
): string[] {
  return createSchemaStatements(assertSqlIdentifier(rowsTable), assertSqlIdentifier(kvTable));
}

export class SqliteRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
> implements RowStorageAdapter<S> {
  private readonly executor: SqliteTransactionExecutor;
  private readonly userID: string;
  private readonly namespace: string;
  private readonly rowsTable: string;
  private readonly kvTable: string;
  private readonly selectColumns: string;
  private readonly upsertStatementHead: string;
  private readonly upsertStatementTail: string;
  private readonly maxRowsPerStatement: number;
  private readonly pendingOperations: PendingOperation<S>[] = [];
  private readonly queue = createSerialQueue();

  private schemaReady: Promise<void> | undefined;

  constructor(input: CreateSqliteRowStoreAdapterInput) {
    this.executor = input.executor;
    this.userID = assertUserID(input.userID);
    this.namespace = assertNamespace(input.namespace);
    this.rowsTable = assertSqlIdentifier(input.rowsTable ?? DEFAULT_ROWS_TABLE);
    this.kvTable = assertSqlIdentifier(input.kvTable ?? DEFAULT_KV_TABLE);
    this.maxRowsPerStatement = this.resolveMaxRowsPerStatement(input.maxRowsPerStatement);
    this.selectColumns = [
      "user_id",
      "namespace",
      "collection",
      "id",
      "parent_id",
      "value_json",
      "committed_timestamp_ms",
      "hlc_wall_ms",
      "hlc_counter",
      "hlc_node_id",
      "tx_id",
      "tombstone",
    ].join(", ");
    this.upsertStatementHead = `INSERT INTO ${this.rowsTable} (
      user_id,
      namespace,
      collection,
      id,
      parent_id,
      value_json,
      committed_timestamp_ms,
      hlc_wall_ms,
      hlc_counter,
      hlc_node_id,
      tx_id,
      tombstone
    )`;
    this.upsertStatementTail = `ON CONFLICT(user_id, namespace, collection, id) DO UPDATE SET
      parent_id = excluded.parent_id,
      value_json = excluded.value_json,
      committed_timestamp_ms = excluded.committed_timestamp_ms,
      hlc_wall_ms = excluded.hlc_wall_ms,
      hlc_counter = excluded.hlc_counter,
      hlc_node_id = excluded.hlc_node_id,
      tx_id = excluded.tx_id,
      tombstone = excluded.tombstone
    WHERE
      excluded.hlc_wall_ms > ${this.rowsTable}.hlc_wall_ms
      OR (
        excluded.hlc_wall_ms = ${this.rowsTable}.hlc_wall_ms
        AND excluded.hlc_counter > ${this.rowsTable}.hlc_counter
      )
      OR (
        excluded.hlc_wall_ms = ${this.rowsTable}.hlc_wall_ms
        AND excluded.hlc_counter = ${this.rowsTable}.hlc_counter
        AND excluded.hlc_node_id > ${this.rowsTable}.hlc_node_id
      )
    RETURNING collection, id, parent_id, committed_timestamp_ms, hlc_wall_ms, hlc_counter, hlc_node_id, tombstone`;
  }

  async query<C extends CollectionId<S>>(query: RowQuery<S, C>): Promise<Array<StoredRow<S, C>>> {
    return this.queue.run(async () => {
      await this.ensureSchema();
      return this.executor.transaction(async (sql) => {
        const whereClauses = ["user_id = ?", "namespace = ?", "collection = ?"];
        const params: unknown[] = [this.userID, this.namespace, query.collectionId];

        if (query.id !== undefined) {
          whereClauses.push("id = ?");
          params.push(query.id);
        }

        if (query.parentId !== undefined) {
          whereClauses.push("parent_id = ?");
          params.push(query.parentId);
        }

        if (query.includeTombstones !== true) {
          whereClauses.push("tombstone = 0");
        }

        const rows = await sql.all<SqliteRowRecord>(
          `SELECT ${this.selectColumns} FROM ${this.rowsTable} WHERE ${whereClauses.join(" AND ")}`,
          params,
        );
        return rows.map((row) => toStoredRow<S>(row) as StoredRow<S, C>);
      });
    });
  }

  async applyRows(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<Array<RowApplyOutcome<S>>> {
    if (rows.length === 0) {
      return [];
    }

    return this.queue.run(async () => {
      await this.ensureSchema();

      return this.executor.transaction(async (sql) => {
        for (const row of rows) {
          if (row.namespace !== this.namespace) {
            throw new Error(
              `Namespace mismatch: adapter namespace is "${this.namespace}" but received "${row.namespace}"`,
            );
          }
        }

        return this.applyRowsInternal(sql, rows);
      });
    });
  }

  async appendPending(operations: ReadonlyArray<PendingOperation<S>>): Promise<void> {
    await this.queue.run(async () => {
      appendPendingOperations(this.pendingOperations, operations);
    });
  }

  async getPending(limit: number): Promise<Array<PendingOperation<S>>> {
    return this.queue.run(async () => {
      return getPendingOperations(this.pendingOperations, limit);
    });
  }

  async removePendingThrough(sequenceInclusive: PendingSequence): Promise<void> {
    await this.queue.run(async () => {
      this.pendingOperations.splice(
        0,
        this.pendingOperations.length,
        ...removePendingOperationsThrough(this.pendingOperations, sequenceInclusive),
      );
    });
  }

  async putKV(key: string, value: unknown): Promise<void> {
    await this.queue.run(async () => {
      await this.ensureSchema();
      await this.executor.transaction(async (sql) => {
        await sql.run(
          `INSERT INTO ${this.kvTable} (user_id, namespace, key, value_json)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(user_id, namespace, key) DO UPDATE SET
            value_json = excluded.value_json`,
          [this.userID, this.namespace, key, toValueJson(value)],
        );
      });
    });
  }

  async getKV<Value = unknown>(key: string): Promise<Value | undefined> {
    return this.queue.run(async () => {
      await this.ensureSchema();
      return this.executor.transaction(async (sql) => {
        const record = await sql.get<{ value_json: string }>(
          `SELECT value_json FROM ${this.kvTable} WHERE user_id = ? AND namespace = ? AND key = ?`,
          [this.userID, this.namespace, key],
        );
        return record ? parseValueJson<Value>(record.value_json) : undefined;
      });
    });
  }

  async deleteKV(key: string): Promise<void> {
    await this.queue.run(async () => {
      await this.ensureSchema();
      await this.executor.transaction(async (sql) => {
        await sql.run(
          `DELETE FROM ${this.kvTable} WHERE user_id = ? AND namespace = ? AND key = ?`,
          [this.userID, this.namespace, key],
        );
      });
    });
  }

  async getRawRow(collectionId: string, id: string): Promise<SqliteRowRecord | undefined> {
    return this.queue.run(async () => {
      await this.ensureSchema();
      return this.executor.transaction(async (sql) => {
        return sql.get<SqliteRowRecord>(
          `SELECT ${this.selectColumns} FROM ${this.rowsTable} WHERE user_id = ? AND namespace = ? AND collection = ? AND id = ?`,
          [this.userID, this.namespace, collectionId, id],
        );
      });
    });
  }

  private async ensureSchema(): Promise<void> {
    if (!this.schemaReady) {
      this.schemaReady = this.executor.transaction(async (sql) => {
        const schemaStatements = createSchemaStatements(this.rowsTable, this.kvTable);
        for (const statement of schemaStatements) {
          await sql.run(statement);
        }
      });

      this.schemaReady = this.schemaReady.catch((error) => {
        this.schemaReady = undefined;
        throw error;
      });
    }

    await this.schemaReady;
  }

  private async applyRowsInternal(
    sql: SqliteStatementExecutor,
    rows: ReadonlyArray<AnyStoredRow<S>>,
  ): Promise<Array<RowApplyOutcome<S>>> {
    if (rows.length === 0) {
      return [];
    }

    const encodedRows = rows.map((row) => toEncodedSqliteRow(row));
    const returningCounts = new Map<string, number>();

    for (let start = 0; start < encodedRows.length; start += this.maxRowsPerStatement) {
      const chunk = encodedRows.slice(start, start + this.maxRowsPerStatement);
      const { statement, params } = this.buildBulkUpsertStatement(chunk);
      const returnedRows = await sql.all<UpsertReturningRow>(statement, params);

      for (const returned of returnedRows) {
        const signature = this.rowSignature({
          collectionId: returned.collection,
          id: returned.id,
          parentId: returned.parent_id,
          committedTimestampMs: Number(returned.committed_timestamp_ms),
          hlcTimestampMs: Number(returned.hlc_wall_ms),
          hlcCounter: Number(returned.hlc_counter),
          hlcDeviceId: returned.hlc_node_id,
          tombstone: returned.tombstone === 1,
        });
        returningCounts.set(signature, (returningCounts.get(signature) ?? 0) + 1);
      }
    }

    return rows.map((row) => {
      const signature = this.rowSignature(row);
      const remainingMatches = returningCounts.get(signature) ?? 0;
      const written = remainingMatches > 0;

      if (written) {
        if (remainingMatches === 1) {
          returningCounts.delete(signature);
        } else {
          returningCounts.set(signature, remainingMatches - 1);
        }
      }

      return {
        written,
        collectionId: row.collectionId,
        id: row.id,
        parentId: row.parentId,
        committedTimestampMs: row.committedTimestampMs,
        hlcTimestampMs: row.hlcTimestampMs,
        hlcCounter: row.hlcCounter,
        hlcDeviceId: row.hlcDeviceId,
        tombstone: row.tombstone,
      };
    });
  }

  private resolveMaxRowsPerStatement(value: number | undefined): number {
    const resolved = value ?? DEFAULT_MAX_ROWS_PER_STATEMENT;
    assertFiniteInteger(resolved, "maxRowsPerStatement");
    if (resolved === 0) {
      throw new Error("Invalid maxRowsPerStatement: expected a positive integer");
    }
    return resolved;
  }

  private buildBulkUpsertStatement(rows: ReadonlyArray<EncodedSqliteRow>): {
    statement: string;
    params: unknown[];
  } {
    const placeholders = rows
      .map(() => `(${Array.from({ length: ROW_BIND_PARAMETER_COUNT }, () => "?").join(", ")})`)
      .join(", ");
    const params: unknown[] = [];

    for (const row of rows) {
      params.push(
        this.userID,
        this.namespace,
        row.collectionId,
        row.id,
        row.parentId,
        row.valueJson,
        row.committedTimestampMs,
        row.hlcTimestampMs,
        row.hlcCounter,
        row.hlcDeviceId,
        row.txId,
        row.tombstone,
      );
    }

    return {
      statement: `${this.upsertStatementHead} VALUES ${placeholders} ${this.upsertStatementTail}`,
      params,
    };
  }

  private rowSignature(row: {
    collectionId: string;
    id: string;
    parentId: string | null;
    committedTimestampMs: number;
    hlcTimestampMs: number;
    hlcCounter: number;
    hlcDeviceId: string;
    tombstone: boolean;
  }): string {
    return JSON.stringify([
      rowKey(row.collectionId, row.id),
      row.parentId,
      row.committedTimestampMs,
      row.hlcTimestampMs,
      row.hlcCounter,
      row.hlcDeviceId,
      row.tombstone ? 1 : 0,
    ]);
  }
}
