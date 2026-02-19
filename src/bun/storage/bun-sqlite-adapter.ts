import { Database } from "bun:sqlite";

import type { CollectionValueMap } from "../../core/types";
import type { SqliteStatementExecutor, SqliteTransactionExecutor } from "../../sqlite";
import { SqliteRowStoreAdapter } from "../../sqlite";

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
    this.database.run("BEGIN IMMEDIATE");
    try {
      const result = await runner(new BunSqliteStatementExecutor(this.database));
      this.database.run("COMMIT");
      return result;
    } catch (error) {
      try {
        this.database.run("ROLLBACK");
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
  kvTable?: string;
}

export class BunSqliteRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
> extends SqliteRowStoreAdapter<S> {
  readonly database: Database;

  constructor(input: CreateBunSqliteRowStoreAdapterInput) {
    const database = input.database ?? new Database(input.path ?? ":memory:");
    const executor = new BunSqliteTransactionExecutor(database);

    super({
      executor,
      userID: input.userID,
      namespace: input.namespace,
      rowsTable: input.rowsTable,
      kvTable: input.kvTable,
    });

    this.database = database;
  }

  close(): void {
    this.database.close(false);
  }
}

export function createBunSqliteRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
>(input: CreateBunSqliteRowStoreAdapterInput): BunSqliteRowStoreAdapter<S> {
  return new BunSqliteRowStoreAdapter<S>(input);
}
