import { describe, expect, test } from "bun:test";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";

import { createEngine } from "../engine";
import { createClockService, type ClockStorageAdapter, type HybridLogicalClock } from "../hlc";
import { createIndexedDbRowStoreAdapter } from "./indexeddb-adapter";

interface RowValue {
  title: string;
}

function createIndexedDbEngine() {
  const dbName = `row-store-test-${crypto.randomUUID()}`;
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
    now: () => 2_000,
  });

  const adapter = createIndexedDbRowStoreAdapter<RowValue>({
    dbName,
    indexedDB,
    IDBKeyRange,
  });
  const engine = createEngine<RowValue>({
    adapter,
    clock,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return {
    adapter,
    engine,
    cleanup: async () => {
      await adapter.deleteDatabase();
    },
  };
}

describe("IndexedDbRowStoreAdapter", () => {
  test("stores and reads rows through Engine", async () => {
    const { adapter, engine, cleanup } = createIndexedDbEngine();

    try {
      const write = await engine.txn([
        { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
      ]);

      expect(write.writes[0]?.hlc).toBe("2000-0-deviceA");

      const read = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
      expect(read.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get",
        row: {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "Dune" },
          hlc: "2000-0-deviceA",
          txID: "tx_1",
          tombstone: false,
        },
      });

      await engine.txn([{ kind: "delete", collection: "books", id: "book-1" }]);

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        hlcWallMs: 2000,
        hlcCounter: 1,
        hlcNodeId: "deviceA",
        tombstone: 1,
      });

      const deleted = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
      expect(deleted.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get",
        row: undefined,
      });
    } finally {
      await cleanup();
    }
  });

  test("delete_all_with_parent only tombstones matching children", async () => {
    const { adapter, engine, cleanup } = createIndexedDbEngine();

    try {
      await engine.txn([
        {
          kind: "put",
          collection: "highlights",
          id: "h-1",
          parentID: "book-1",
          value: { title: "a" },
        },
        {
          kind: "put",
          collection: "highlights",
          id: "h-2",
          parentID: "book-1",
          value: { title: "b" },
        },
        {
          kind: "put",
          collection: "highlights",
          id: "h-3",
          parentID: "book-2",
          value: { title: "c" },
        },
      ]);

      const deleteResult = await engine.txn([
        {
          kind: "delete_all_with_parent",
          collection: "highlights",
          parentID: "book-1",
        },
      ]);

      expect(deleteResult.writes).toEqual([
        {
          collection: "highlights",
          id: "h-1",
          parentID: "book-1",
          hlc: "2000-3-deviceA",
          tombstone: true,
        },
        {
          collection: "highlights",
          id: "h-2",
          parentID: "book-1",
          hlc: "2000-4-deviceA",
          tombstone: true,
        },
      ]);

      const bookOneChildren = await engine.txn([
        {
          kind: "get_all_with_parent",
          collection: "highlights",
          parentID: "book-1",
        },
      ]);
      expect(bookOneChildren.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get_all_with_parent",
        rows: [],
      });

      const bookTwoChildren = await engine.txn([
        {
          kind: "get_all_with_parent",
          collection: "highlights",
          parentID: "book-2",
        },
      ]);
      expect(bookTwoChildren.readResults[0]).toEqual({
        opIndex: 0,
        kind: "get_all_with_parent",
        rows: [
          {
            collection: "highlights",
            id: "h-3",
            parentID: "book-2",
            value: { title: "c" },
            hlc: "2000-2-deviceA",
            txID: "tx_1",
            tombstone: false,
          },
        ],
      });

      expect((await adapter.getRawRow("highlights", "h-1"))?.tombstone).toBe(1);
      expect((await adapter.getRawRow("highlights", "h-2"))?.tombstone).toBe(1);
      expect((await adapter.getRawRow("highlights", "h-3"))?.tombstone).toBe(0);
      expect(await adapter.getRawRow("highlights", "h-3")).toMatchObject({
        hlcWallMs: 2000,
        hlcCounter: 2,
        hlcNodeId: "deviceA",
        tombstone: 0,
      });
    } finally {
      await cleanup();
    }
  });
});
