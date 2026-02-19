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

function createIndexedDbEngine(dbName = `row-store-test-${crypto.randomUUID()}`) {
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
    dbName,
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
      const write = await engine.put("books", "book-1", { title: "Dune" });

      expect(write.hlcTimestampMs).toBe(2000);
      expect(write.hlcCounter).toBe(0);

      const read = await engine.get("books", "book-1");
      expect(read).toMatchObject({
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

      await engine.delete("books", "book-1");

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        committedTimestampMs: 2000,
        hlcTimestampMs: 2000,
        hlcCounter: 1,
        hlcDeviceId: "deviceA",
        tombstone: 1,
      });

      const deleted = await engine.get("books", "book-1");
      expect(deleted).toBeUndefined();
    } finally {
      await cleanup();
    }
  });

  test("deleteAllWithParent only tombstones matching children", async () => {
    const { adapter, engine, cleanup } = createIndexedDbEngine();

    try {
      await engine.batchLocal([
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
      ]);

      const deleted = await engine.deleteAllWithParent("highlights", "book-1");

      expect(deleted).toHaveLength(2);
      expect(deleted.every((entry) => entry.tombstone)).toBe(true);

      const bookOneChildren = await engine.getAllWithParent("highlights", "book-1");
      expect(bookOneChildren).toEqual([]);

      const bookTwoChildren = await engine.getAllWithParent("highlights", "book-2");
      expect(bookTwoChildren).toHaveLength(1);
      expect(bookTwoChildren[0]).toMatchObject({
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

  test("persists pending operations across adapter instances", async () => {
    const dbName = `row-store-test-${crypto.randomUUID()}`;
    const first = createIndexedDbEngine(dbName);

    try {
      await first.engine.batchLocal([
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
        { type: "put", collectionId: "books", id: "book-2", data: { title: "Messiah" } },
      ]);

      expect((await first.engine.getPending(10)).map((operation) => operation.sequence)).toEqual([
        1, 2,
      ]);
      first.adapter.close();

      const second = createIndexedDbEngine(dbName);
      try {
        expect((await second.engine.getPending(10)).map((operation) => operation.sequence)).toEqual(
          [1, 2],
        );

        await second.engine.put("books", "book-3", { title: "Children of Dune" });
        expect((await second.engine.getPending(10)).map((operation) => operation.sequence)).toEqual(
          [1, 2, 3],
        );
      } finally {
        await second.cleanup();
      }
    } finally {
      try {
        await first.cleanup();
      } catch {
        // Ignore: database may already be deleted by the second cleanup.
      }
    }
  });
});
