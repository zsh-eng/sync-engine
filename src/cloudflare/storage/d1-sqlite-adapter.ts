import type { SqliteStatementExecutor, SqliteTransactionExecutor } from "../../sqlite";
import type { CollectionValueMap } from "../../core/types";
import { SqliteRowStoreAdapter } from "../../sqlite";

function resultRows<Row>(result: D1ResultLike<Row> | undefined): Row[] {
  return Array.isArray(result?.results) ? result.results : [];
}

export interface D1ResultLike<Row = unknown> {
  results?: Row[] | null;
}

export interface D1PreparedStatementLike {
  bind(...values: unknown[]): D1PreparedStatementLike;
}

export interface D1DatabaseLike {
  prepare(sql: string): D1PreparedStatementLike;
  batch<Row = unknown>(
    statements: ReadonlyArray<D1PreparedStatementLike>,
  ): Promise<ReadonlyArray<D1ResultLike<Row>>>;
}

class D1SqliteStatementExecutor implements SqliteStatementExecutor {
  private pendingRuns: D1PreparedStatementLike[] = [];

  constructor(private readonly database: D1DatabaseLike) {}

  async run(sql: string, params: ReadonlyArray<unknown> = []): Promise<void> {
    this.pendingRuns.push(this.database.prepare(sql).bind(...params));
  }

  async get<Row = unknown>(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<Row | undefined> {
    await this.flushPendingRuns();
    const rows = await this.executeAll<Row>(sql, params);
    return rows[0] as Row | undefined;
  }

  async all<Row = unknown>(sql: string, params: ReadonlyArray<unknown> = []): Promise<Row[]> {
    await this.flushPendingRuns();
    return this.executeAll<Row>(sql, params);
  }

  async flushPendingRuns(): Promise<void> {
    if (this.pendingRuns.length === 0) {
      return;
    }

    const statements = this.pendingRuns;
    this.pendingRuns = [];
    await this.database.batch(statements);
  }

  clearPendingRuns(): void {
    this.pendingRuns = [];
  }

  private async executeAll<Row>(sql: string, params: ReadonlyArray<unknown> = []): Promise<Row[]> {
    const statement = this.database.prepare(sql).bind(...params);
    const [result] = await this.database.batch<Row>([statement]);
    return resultRows(result);
  }
}

class D1SqliteTransactionExecutor implements SqliteTransactionExecutor {
  constructor(private readonly database: D1DatabaseLike) {}

  async transaction<Result>(
    runner: (executor: SqliteStatementExecutor) => Promise<Result>,
  ): Promise<Result> {
    const executor = new D1SqliteStatementExecutor(this.database);

    try {
      const result = await runner(executor);
      await executor.flushPendingRuns();
      return result;
    } catch (error) {
      // D1 does not expose explicit rollback control; only discard queued run statements.
      executor.clearPendingRuns();
      throw error;
    }
  }
}

export interface CreateD1SqliteRowStoreAdapterInput {
  database: D1DatabaseLike;
  userID: string;
  namespace: string;
  rowsTable?: string;
  kvTable?: string;
  pendingTable?: string;
}

export class D1SqliteRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
> extends SqliteRowStoreAdapter<S> {
  readonly database: D1DatabaseLike;

  constructor(input: CreateD1SqliteRowStoreAdapterInput) {
    const executor = new D1SqliteTransactionExecutor(input.database);

    super({
      executor,
      userID: input.userID,
      namespace: input.namespace,
      rowsTable: input.rowsTable,
      kvTable: input.kvTable,
      pendingTable: input.pendingTable,
    });

    this.database = input.database;
  }
}

export function createD1SqliteRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
>(input: CreateD1SqliteRowStoreAdapterInput): D1SqliteRowStoreAdapter<S> {
  return new D1SqliteRowStoreAdapter<S>(input);
}
