import { describe, expect, test } from "bun:test";
import { IDBKeyRange, indexedDB } from "fake-indexeddb";

import { createEngine } from "../../core/engine";
import {
  createClockService,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "../../core/hlc";
import { createIndexedDbRowStoreAdapter } from "./indexeddb-adapter";

interface RowValue {
  title: string;
}

interface Collections {
  books: RowValue;
  highlights: RowValue;
}

const TEST_NAMESPACE = "books-app";

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

  const adapter = createIndexedDbRowStoreAdapter<Collections>({
    dbName,
    namespace: TEST_NAMESPACE,
    indexedDB,
    IDBKeyRange,
  });
  const engine = createEngine<Collections>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
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
      const write = await engine.execute([
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
      ] as const);

      expect(write[0]?.hlcTimestampMs).toBe(2000);
      expect(write[0]?.hlcCounter).toBe(0);

      const read = await engine.execute([
        { type: "get", collectionId: "books", id: "book-1" },
      ] as const);
      expect(read[0]).toMatchObject({
        namespace: TEST_NAMESPACE,
        collectionId: "books",
        id: "book-1",
        parentId: null,
        data: { title: "Dune" },
        txId: "tx_1",
        tombstone: false,
        committedTimestampMs: 2000,
        hlcTimestampMs: 2000,
        hlcCounter: 0,
        hlcDeviceId: "deviceA",
      });

      await engine.execute([{ type: "delete", collectionId: "books", id: "book-1" }] as const);

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        committedTimestampMs: 2000,
        hlcTimestampMs: 2000,
        hlcCounter: 1,
        hlcDeviceId: "deviceA",
        tombstone: 1,
      });

      const deleted = await engine.execute([
        { type: "get", collectionId: "books", id: "book-1" },
      ] as const);
      expect(deleted[0]).toBeUndefined();
    } finally {
      await cleanup();
    }
  });

  test("deleteAllWithParent only tombstones matching children", async () => {
    const { adapter, engine, cleanup } = createIndexedDbEngine();

    try {
      await engine.execute([
        {
          type: "put",
          collectionId: "highlights",
          id: "h-1",
          parentId: "book-1",
          data: { title: "a" },
        },
        {
          type: "put",
          collectionId: "highlights",
          id: "h-2",
          parentId: "book-1",
          data: { title: "b" },
        },
        {
          type: "put",
          collectionId: "highlights",
          id: "h-3",
          parentId: "book-2",
          data: { title: "c" },
        },
      ] as const);

      const deleted = await engine.execute([
        {
          type: "deleteAllWithParent",
          collectionId: "highlights",
          parentId: "book-1",
        },
      ] as const);

      expect(deleted[0]).toHaveLength(2);
      expect(deleted[0].every((entry) => entry.tombstone)).toBe(true);

      const bookOneChildren = await engine.execute([
        {
          type: "getAllWithParent",
          collectionId: "highlights",
          parentId: "book-1",
        },
      ] as const);
      expect(bookOneChildren[0]).toEqual([]);

      const bookTwoChildren = await engine.execute([
        {
          type: "getAllWithParent",
          collectionId: "highlights",
          parentId: "book-2",
        },
      ] as const);
      expect(bookTwoChildren[0]).toHaveLength(1);
      expect(bookTwoChildren[0][0]).toMatchObject({
        id: "h-3",
        parentId: "book-2",
        data: { title: "c" },
        tombstone: false,
      });

      expect((await adapter.getRawRow("highlights", "h-1"))?.tombstone).toBe(1);
      expect((await adapter.getRawRow("highlights", "h-2"))?.tombstone).toBe(1);
      expect((await adapter.getRawRow("highlights", "h-3"))?.tombstone).toBe(0);
    } finally {
      await cleanup();
    }
  });
});
