import { Database } from "bun:sqlite";

import type {
  SqliteRowRecord,
  SqliteStatementExecutor,
  SqliteTransactionExecutor,
} from "../../sqlite";
import { SqliteRowStoreAdapter } from "../../sqlite";

function assertSqlIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }

  return value;
}

class BunSqliteStatementExecutor implements SqliteStatementExecutor {
  constructor(private readonly database: Database) {}

  async run(sql: string, params: ReadonlyArray<unknown> = []): Promise<void> {
    this.database
      .query(sql)
      .run(...([...params] as Array<string | number | bigint | boolean | Uint8Array | null>));
  }

  async get<Row = unknown>(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<Row | undefined> {
    const result = this.database
      .query(sql)
      .get(...([...params] as Array<string | number | bigint | boolean | Uint8Array | null>));
    return (result ?? undefined) as Row | undefined;
  }

  async all<Row = unknown>(sql: string, params: ReadonlyArray<unknown> = []): Promise<Row[]> {
    const result = this.database
      .query(sql)
      .all(...([...params] as Array<string | number | bigint | boolean | Uint8Array | null>));
    return result as Row[];
  }
}

class BunSqliteTransactionExecutor implements SqliteTransactionExecutor {
  constructor(private readonly database: Database) {}

  async transaction<Result>(
    runner: (executor: SqliteStatementExecutor) => Promise<Result>,
  ): Promise<Result> {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = await runner(new BunSqliteStatementExecutor(this.database));
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      try {
        this.database.exec("ROLLBACK");
      } catch {
        // Ignore rollback errors; surface the original failure.
      }
      throw error;
    }
  }
}

export interface CreateBunSqliteRowStoreAdapterInput {
  userID: string;
  namespace: string;
  database?: Database;
  path?: string;
  rowsTable?: string;
}

export class BunSqliteRowStoreAdapter<Value = unknown> extends SqliteRowStoreAdapter<Value> {
  readonly database: Database;
  private readonly userID: string;
  private readonly namespace: string;
  private readonly rowsTable: string;

  constructor(input: CreateBunSqliteRowStoreAdapterInput) {
    const database = input.database ?? new Database(input.path ?? ":memory:");
    const executor = new BunSqliteTransactionExecutor(database);

    super({
      executor,
      userID: input.userID,
      namespace: input.namespace,
      rowsTable: input.rowsTable,
    });

    this.database = database;
    this.userID = input.userID;
    this.namespace = input.namespace;
    this.rowsTable = assertSqlIdentifier(input.rowsTable ?? "rows");
  }

  async getRawRow(collection: string, id: string): Promise<SqliteRowRecord | undefined> {
    const row = this.database
      .query(
        `SELECT
          user_id,
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
         FROM ${this.rowsTable}
         WHERE user_id = ? AND namespace = ? AND collection = ? AND id = ?`,
      )
      .get(this.userID, this.namespace, collection, id);
    return (row ?? undefined) as SqliteRowRecord | undefined;
  }

  close(): void {
    this.database.close(false);
  }
}

export function createBunSqliteRowStoreAdapter<Value = unknown>(
  input: CreateBunSqliteRowStoreAdapterInput,
): BunSqliteRowStoreAdapter<Value> {
  return new BunSqliteRowStoreAdapter<Value>(input);
}
