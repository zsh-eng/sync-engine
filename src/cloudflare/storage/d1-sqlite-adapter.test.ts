import { Database } from "bun:sqlite";
import { describe, expect, test } from "bun:test";

import { createEngine } from "../../core/engine";
import {
  createClockService,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "../../core/hlc";
import type { AnyStoredRow } from "../../core/types";
import type { D1DatabaseLike, D1PreparedStatementLike, D1ResultLike } from "./d1-sqlite-adapter";
import { createD1SqliteRowStoreAdapter } from "./d1-sqlite-adapter";

interface RowValue {
  title: string;
}

interface Collections {
  books: RowValue;
}

const TEST_NAMESPACE = "books-app";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

type SqliteBindValue = string | number | bigint | boolean | Uint8Array | null;

function toSqliteBindValues(params: ReadonlyArray<unknown>): SqliteBindValue[] {
  return [...params] as SqliteBindValue[];
}

function statementReturnsRows(sql: string): boolean {
  return /^\s*(SELECT|WITH)\b/i.test(sql) || /\bRETURNING\b/i.test(sql);
}

interface ExecutablePreparedStatement extends D1PreparedStatementLike {
  execute<Row>(): D1ResultLike<Row>;
}

class FakeD1PreparedStatement implements ExecutablePreparedStatement {
  constructor(
    private readonly database: Database,
    private readonly sql: string,
    private readonly params: ReadonlyArray<unknown> = [],
  ) {}

  bind(...values: unknown[]): D1PreparedStatementLike {
    return new FakeD1PreparedStatement(this.database, this.sql, values);
  }

  execute<Row>(): D1ResultLike<Row> {
    const query = this.database.query(this.sql);
    const params = toSqliteBindValues(this.params);

    if (statementReturnsRows(this.sql)) {
      return {
        results: query.all(...params) as Row[],
      };
    }

    query.run(...params);
    return { results: [] };
  }
}

class FakeD1Database implements D1DatabaseLike {
  readonly batchSizes: number[] = [];
  private readonly database: Database;

  constructor() {
    this.database = new Database(":memory:");
  }

  prepare(sql: string): D1PreparedStatementLike {
    return new FakeD1PreparedStatement(this.database, sql);
  }

  async batch<Row = unknown>(
    statements: ReadonlyArray<D1PreparedStatementLike>,
  ): Promise<ReadonlyArray<D1ResultLike<Row>>> {
    this.batchSizes.push(statements.length);
    return statements.map((statement) => (statement as ExecutablePreparedStatement).execute<Row>());
  }

  close(): void {
    this.database.close(false);
  }
}

function remoteRow(
  clock: HybridLogicalClock,
  title: string,
  txId: string,
): AnyStoredRow<Collections> {
  const parsed = parseClock(clock);
  return {
    namespace: TEST_NAMESPACE,
    collectionId: "books",
    id: "book-1",
    parentId: null,
    data: { title },
    txId,
    tombstone: false,
    committedTimestampMs: parsed.wallMs,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  };
}

function createD1SqliteEngine() {
  let storedClock: HybridLogicalClock | undefined;
  let txCounter = 0;

  const clockStorage: ClockStorageAdapter = {
    read: () => storedClock,
    write: (clock) => {
      storedClock = clock;
    },
  };

  const clock = createClockService({
    nodeId: "deviceA",
    storage: clockStorage,
    now: () => 3_000,
  });

  const database = new FakeD1Database();
  const adapter = createD1SqliteRowStoreAdapter<Collections>({
    database,
    userID: "user-1",
    namespace: TEST_NAMESPACE,
  });
  const engine = createEngine<Collections>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return {
    adapter,
    database,
    engine,
    cleanup: () => {
      database.close();
    },
  };
}

describe("D1SqliteRowStoreAdapter", () => {
  test("reports write outcomes when the same row is updated twice", async () => {
    const { engine, cleanup } = createD1SqliteEngine();

    try {
      const write = await engine.batchLocal([
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune Messiah" } },
      ]);

      expect(write[0]).toMatchObject({
        collectionId: "books",
        id: "book-1",
        hlcTimestampMs: 3000,
        hlcCounter: 0,
        applied: true,
      });
      expect(write[1]).toMatchObject({
        collectionId: "books",
        id: "book-1",
        hlcTimestampMs: 3000,
        hlcCounter: 1,
        applied: true,
      });
    } finally {
      cleanup();
    }
  });

  test("stores rows and preserves metadata", async () => {
    const { adapter, database, engine, cleanup } = createD1SqliteEngine();

    try {
      await engine.put("books", "book-1", { title: "Dune" });
      await engine.delete("books", "book-1");

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        committed_timestamp_ms: 3000,
        hlc_wall_ms: 3000,
        hlc_counter: 1,
        hlc_node_id: "deviceA",
        tombstone: 1,
      });

      expect(database.batchSizes).toContain(8);
    } finally {
      cleanup();
    }
  });

  test("applies LWW tie-break with node ID for equal wall/counter", async () => {
    const { engine, cleanup } = createD1SqliteEngine();

    try {
      const firstApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceA"), "from A", "tx_a"),
      ]);
      expect(firstApply.appliedCount).toBe(1);

      const secondApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceZ"), "from Z", "tx_z"),
      ]);
      expect(secondApply.appliedCount).toBe(1);

      const staleApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceB"), "from B", "tx_b"),
      ]);
      expect(staleApply.appliedCount).toBe(0);

      const read = await engine.get("books", "book-1");
      expect(read).toMatchObject({
        data: { title: "from Z" },
        txId: "tx_z",
      });
    } finally {
      cleanup();
    }
  });
});
