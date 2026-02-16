import { formatClock, parseClock } from "../../core/hlc";
import type {
  RowStoreAdapter,
  RowStoreAdapterTransaction,
  StoredRow,
  WriteOutcome,
} from "../../core/storage/types";

const DEFAULT_ROWS_TABLE = "rows";
const ROW_BIND_PARAMETER_COUNT = 11;
const DEFAULT_MAX_ROWS_PER_STATEMENT = 90;

function rowKey(collection: string, id: string): string {
  return `${collection}::${id}`;
}

function assertNamespace(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Invalid namespace: expected a non-empty string");
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
  namespace: string;
  collection: string;
  id: string;
  parent_id: string | null;
  value_json: string | null;
  hlc: string;
  hlc_wall_ms: number;
  hlc_counter: number;
  hlc_node_id: string;
  tx_id: string;
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
  namespace: string;
  rowsTable?: string;
  maxRowsPerStatement?: number;
}

interface EncodedSqliteRow {
  collection: string;
  id: string;
  parentID: string | null;
  valueJson: string | null;
  hlc: string;
  hlcWallMs: number;
  hlcCounter: number;
  hlcNodeId: string;
  txID: string;
  tombstone: 0 | 1;
}

interface UpsertReturningRow {
  collection: string;
  id: string;
  parent_id: string | null;
  hlc: string;
  tx_id: string;
  tombstone: 0 | 1;
}

function toStoredRow<Value>(row: SqliteRowRecord): StoredRow<Value> {
  const hlcWallMs = Number(row.hlc_wall_ms);
  const hlcCounter = Number(row.hlc_counter);

  assertFiniteInteger(hlcWallMs, "hlc_wall_ms");
  assertFiniteInteger(hlcCounter, "hlc_counter");

  return {
    collection: row.collection,
    id: row.id,
    parentID: row.parent_id,
    value: row.value_json === null ? null : parseValueJson<Value>(row.value_json),
    hlc: formatClock({
      wallMs: hlcWallMs,
      counter: hlcCounter,
      nodeId: row.hlc_node_id,
    }) as StoredRow<Value>["hlc"],
    txID: row.tx_id,
    tombstone: row.tombstone === 1,
  };
}

function toEncodedSqliteRow<Value>(row: StoredRow<Value>): EncodedSqliteRow {
  const parsedClock = parseClock(row.hlc);
  return {
    collection: row.collection,
    id: row.id,
    parentID: row.parentID,
    valueJson: row.value === null ? null : toValueJson(row.value),
    hlc: row.hlc,
    hlcWallMs: parsedClock.wallMs,
    hlcCounter: parsedClock.counter,
    hlcNodeId: parsedClock.nodeId,
    txID: row.txID,
    tombstone: row.tombstone ? 1 : 0,
  };
}

function createRowsSchemaStatements(rowsTable: string): string[] {
  return [
    `CREATE TABLE IF NOT EXISTS ${rowsTable} (
      namespace TEXT NOT NULL,
      collection TEXT NOT NULL,
      id TEXT NOT NULL,
      parent_id TEXT,
      value_json TEXT,
      hlc TEXT NOT NULL,
      hlc_wall_ms INTEGER NOT NULL,
      hlc_counter INTEGER NOT NULL,
      hlc_node_id TEXT NOT NULL,
      tx_id TEXT NOT NULL,
      tombstone INTEGER NOT NULL CHECK (tombstone IN (0, 1)),
      PRIMARY KEY (namespace, collection, id)
    )`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_namespace_collection_idx ON ${rowsTable} (namespace, collection)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_namespace_collection_parent_idx ON ${rowsTable} (namespace, collection, parent_id)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_namespace_collection_tombstone_idx ON ${rowsTable} (namespace, collection, tombstone)`,
    `CREATE INDEX IF NOT EXISTS ${rowsTable}_namespace_hlc_idx ON ${rowsTable} (namespace, hlc_wall_ms, hlc_counter, hlc_node_id)`,
  ];
}

export function createSqliteRowStoreSchemaStatements(rowsTable = DEFAULT_ROWS_TABLE): string[] {
  return createRowsSchemaStatements(assertSqlIdentifier(rowsTable));
}

export class SqliteRowStoreAdapter<Value = unknown> implements RowStoreAdapter<Value> {
  private readonly executor: SqliteTransactionExecutor;
  private readonly namespace: string;
  private readonly rowsTable: string;
  private readonly selectColumns: string;
  private readonly upsertStatementHead: string;
  private readonly upsertStatementTail: string;
  private readonly maxRowsPerStatement: number;

  private schemaReady: Promise<void> | undefined;
  private queue: Promise<void> = Promise.resolve();

  constructor(input: CreateSqliteRowStoreAdapterInput) {
    this.executor = input.executor;
    this.namespace = assertNamespace(input.namespace);
    this.rowsTable = assertSqlIdentifier(input.rowsTable ?? DEFAULT_ROWS_TABLE);
    this.maxRowsPerStatement = this.resolveMaxRowsPerStatement(input.maxRowsPerStatement);
    this.selectColumns = [
      "namespace",
      "collection",
      "id",
      "parent_id",
      "value_json",
      "hlc",
      "hlc_wall_ms",
      "hlc_counter",
      "hlc_node_id",
      "tx_id",
      "tombstone",
    ].join(", ");
    this.upsertStatementHead = `INSERT INTO ${this.rowsTable} (
      namespace,
      collection,
      id,
      parent_id,
      value_json,
      hlc,
      hlc_wall_ms,
      hlc_counter,
      hlc_node_id,
      tx_id,
      tombstone
    )`;
    this.upsertStatementTail = `ON CONFLICT(namespace, collection, id) DO UPDATE SET
      parent_id = excluded.parent_id,
      value_json = excluded.value_json,
      hlc = excluded.hlc,
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
    RETURNING collection, id, parent_id, hlc, tx_id, tombstone`;
  }

  async txn<Result>(
    runner: (tx: RowStoreAdapterTransaction<Value>) => Promise<Result>,
  ): Promise<Result> {
    return this.enqueue(async () => {
      await this.ensureSchema();

      return this.executor.transaction(async (sql) => {
        const tx: RowStoreAdapterTransaction<Value> = {
          getAll: async (collection) => {
            const rows = await sql.all<SqliteRowRecord>(
              `SELECT ${this.selectColumns} FROM ${this.rowsTable} WHERE namespace = ? AND collection = ?`,
              [this.namespace, collection],
            );
            return rows.map((row) => toStoredRow<Value>(row));
          },

          get: async (collection, id) => {
            const row = await sql.get<SqliteRowRecord>(
              `SELECT ${this.selectColumns} FROM ${this.rowsTable} WHERE namespace = ? AND collection = ? AND id = ?`,
              [this.namespace, collection, id],
            );
            return row ? toStoredRow<Value>(row) : undefined;
          },

          getAllWithParent: async (collection, parentID) => {
            const rows = await sql.all<SqliteRowRecord>(
              `SELECT ${this.selectColumns} FROM ${this.rowsTable} WHERE namespace = ? AND collection = ? AND parent_id = ?`,
              [this.namespace, collection, parentID],
            );
            return rows.map((row) => toStoredRow<Value>(row));
          },

          applyRows: async (rows) => {
            return this.applyRows(sql, rows);
          },
        };

        return runner(tx);
      });
    });
  }

  private async ensureSchema(): Promise<void> {
    if (!this.schemaReady) {
      this.schemaReady = this.executor.transaction(async (sql) => {
        const schemaStatements = createRowsSchemaStatements(this.rowsTable);
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

  private async enqueue<Result>(operation: () => Promise<Result>): Promise<Result> {
    const previous = this.queue;
    let release: () => void = () => undefined;

    this.queue = new Promise<void>((resolve) => {
      release = resolve;
    });

    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  private async applyRows(
    sql: SqliteStatementExecutor,
    rows: ReadonlyArray<StoredRow<Value>>,
  ): Promise<WriteOutcome[]> {
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
          collection: returned.collection,
          id: returned.id,
          parentID: returned.parent_id,
          hlc: returned.hlc as StoredRow<Value>["hlc"],
          txID: returned.tx_id,
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
        collection: row.collection,
        id: row.id,
        parentID: row.parentID,
        hlc: row.hlc,
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
      .map(() => `(${new Array(ROW_BIND_PARAMETER_COUNT).fill("?").join(", ")})`)
      .join(", ");
    const params: unknown[] = [];

    for (const row of rows) {
      params.push(
        this.namespace,
        row.collection,
        row.id,
        row.parentID,
        row.valueJson,
        row.hlc,
        row.hlcWallMs,
        row.hlcCounter,
        row.hlcNodeId,
        row.txID,
        row.tombstone,
      );
    }

    return {
      statement: `${this.upsertStatementHead} VALUES ${placeholders} ${this.upsertStatementTail}`,
      params,
    };
  }

  private rowSignature(row: {
    collection: string;
    id: string;
    parentID: string | null;
    hlc: StoredRow<Value>["hlc"];
    txID: string;
    tombstone: boolean;
  }): string {
    return JSON.stringify([
      rowKey(row.collection, row.id),
      row.parentID,
      row.hlc,
      row.txID,
      row.tombstone ? 1 : 0,
    ]);
  }
}
