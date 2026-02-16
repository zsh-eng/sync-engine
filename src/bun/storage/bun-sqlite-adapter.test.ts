import { describe, expect, test } from "bun:test";

import { createEngine } from "../../core/engine";
import {
  createClockService,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "../../core/hlc";
import { createBunSqliteRowStoreAdapter } from "./bun-sqlite-adapter";

interface RowValue {
  title: string;
}

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function createBunSqliteEngine() {
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

  const adapter = createBunSqliteRowStoreAdapter<RowValue>({
    userID: "user-1",
    namespace: "books-app",
  });
  const engine = createEngine<RowValue>({
    adapter,
    clock,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return {
    adapter,
    engine,
    cleanup: () => {
      adapter.close();
    },
  };
}

describe("BunSqliteRowStoreAdapter", () => {
  test("reports write outcomes when the same row is updated twice in one txn", async () => {
    const { engine, cleanup } = createBunSqliteEngine();

    try {
      const writeResult = await engine.txn([
        { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
        { kind: "put", collection: "books", id: "book-1", value: { title: "Dune Messiah" } },
      ]);

      expect(writeResult.writes).toEqual([
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          hlc: asClock("3000-0-deviceA"),
          tombstone: false,
        },
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          hlc: asClock("3000-1-deviceA"),
          tombstone: false,
        },
      ]);

      const read = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
      expect(read.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get",
        row: {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "Dune Messiah" },
          hlc: asClock("3000-1-deviceA"),
          txID: "tx_1",
          tombstone: false,
        },
      });
    } finally {
      cleanup();
    }
  });

  test("stores rows and preserves row metadata", async () => {
    const { adapter, engine, cleanup } = createBunSqliteEngine();

    try {
      const write = await engine.txn([
        { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
      ]);

      expect(write.writes[0]?.hlc).toBe(asClock("3000-0-deviceA"));

      const read = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
      expect(read.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get",
        row: {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "Dune" },
          hlc: asClock("3000-0-deviceA"),
          txID: "tx_1",
          tombstone: false,
        },
      });

      await engine.txn([{ kind: "delete", collection: "books", id: "book-1" }]);

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        hlc_wall_ms: 3000,
        hlc_counter: 1,
        hlc_node_id: "deviceA",
        tombstone: 1,
      });
    } finally {
      cleanup();
    }
  });

  test("applies LWW tie-break with node ID for equal wall/counter", async () => {
    const { adapter, engine, cleanup } = createBunSqliteEngine();

    try {
      const firstApply = await engine.applyRemote([
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "from A" },
          hlc: asClock("9000-2-deviceA"),
          txID: "tx_remote_a",
          tombstone: false,
        },
      ]);
      expect(firstApply.appliedCount).toBe(1);

      const secondApply = await engine.applyRemote([
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "from Z" },
          hlc: asClock("9000-2-deviceZ"),
          txID: "tx_remote_z",
          tombstone: false,
        },
      ]);
      expect(secondApply.appliedCount).toBe(1);

      const staleApply = await engine.applyRemote([
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "from B" },
          hlc: asClock("9000-2-deviceB"),
          txID: "tx_remote_b",
          tombstone: false,
        },
      ]);
      expect(staleApply.appliedCount).toBe(0);

      const read = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
      expect(read.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get",
        row: {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "from Z" },
          hlc: asClock("9000-2-deviceZ"),
          txID: "tx_remote_z",
          tombstone: false,
        },
      });
    } finally {
      cleanup();
    }
  });
});
