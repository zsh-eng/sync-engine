import { describe, expect, test } from "bun:test";

import { createClockService, type ClockStorageAdapter, type HybridLogicalClock } from "../hlc";
import { createInMemoryRowStoreAdapter } from "./in-memory-adapter";
import { createRowStore } from "./row-store";
import type { RowStoreOperation, StoredRow } from "./types";

interface BookValue {
  title: string;
}

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function createTestRowStore(input: { seedRows?: ReadonlyArray<StoredRow<BookValue>> } = {}) {
  let stored: HybridLogicalClock | undefined;
  let txCounter = 0;

  const clockStorage: ClockStorageAdapter = {
    read: () => stored,
    write: (clock) => {
      stored = clock;
    },
  };

  const clock = createClockService({
    nodeId: "deviceA",
    storage: clockStorage,
    now: () => 1_000,
  });

  const adapter = createInMemoryRowStoreAdapter<BookValue>({
    seedRows: input.seedRows,
  });
  const rowStore = createRowStore<BookValue>({
    adapter,
    clock,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return { rowStore, adapter };
}

function getResultAt<Value>(
  result: Awaited<ReturnType<ReturnType<typeof createTestRowStore>["rowStore"]["txn"]>>,
  index: number,
) {
  const readResult = result.readResults[index];
  if (!readResult) {
    throw new Error(`Missing read result at index ${index}`);
  }
  return readResult;
}

describe("RowStore core behavior", () => {
  test("assigns HLC values to writes and returns reads in op order", async () => {
    const { rowStore } = createTestRowStore();

    const writeResult = await rowStore.txn([
      {
        kind: "put",
        collection: "books",
        id: "book-1",
        value: { title: "Dune" },
      },
    ]);

    expect(writeResult.txID).toBe("tx_1");
    expect(writeResult.writes).toEqual([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        hlc: "1000-0-deviceA",
        tombstone: false,
      },
    ]);

    const readResult = await rowStore.txn([
      { kind: "get", collection: "books", id: "book-1" },
      { kind: "get_all", collection: "books" },
    ]);

    expect(readResult.txID).toBe("tx_2");
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Dune" },
        hlc: "1000-0-deviceA",
        txID: "tx_1",
        tombstone: false,
      },
    });
    expect(getResultAt(readResult, 1)).toEqual({
      opIndex: 1,
      kind: "get_all",
      rows: [
        {
          collection: "books",
          id: "book-1",
          parentID: null,
          value: { title: "Dune" },
          hlc: "1000-0-deviceA",
          txID: "tx_1",
          tombstone: false,
        },
      ],
    });
  });

  test("delete and delete_all_with_parent create tombstones and hide rows from reads", async () => {
    const { rowStore } = createTestRowStore();

    const setupOps: RowStoreOperation<BookValue>[] = [
      { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
      {
        kind: "put",
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        value: { title: "first highlight" },
      },
      {
        kind: "put",
        collection: "highlights",
        id: "h-2",
        parentID: "book-1",
        value: { title: "second highlight" },
      },
    ];

    await rowStore.txn(setupOps);

    const deleteResult = await rowStore.txn([
      { kind: "delete", collection: "books", id: "book-1" },
      {
        kind: "delete_all_with_parent",
        collection: "highlights",
        parentID: "book-1",
      },
    ]);

    expect(deleteResult.txID).toBe("tx_2");
    expect(deleteResult.writes).toEqual([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        hlc: "1000-3-deviceA",
        tombstone: true,
      },
      {
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        hlc: "1000-4-deviceA",
        tombstone: true,
      },
      {
        collection: "highlights",
        id: "h-2",
        parentID: "book-1",
        hlc: "1000-5-deviceA",
        tombstone: true,
      },
    ]);

    const postDelete = await rowStore.txn([
      { kind: "get", collection: "books", id: "book-1" },
      {
        kind: "get_all_with_parent",
        collection: "highlights",
        parentID: "book-1",
      },
      { kind: "get_all", collection: "highlights" },
    ]);

    expect(getResultAt(postDelete, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: undefined,
    });
    expect(getResultAt(postDelete, 1)).toEqual({
      opIndex: 1,
      kind: "get_all_with_parent",
      rows: [],
    });
    expect(getResultAt(postDelete, 2)).toEqual({
      opIndex: 2,
      kind: "get_all",
      rows: [],
    });
  });

  test("preserves existing parentID when put omits parentID", async () => {
    const { rowStore } = createTestRowStore();

    await rowStore.txn([
      {
        kind: "put",
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        value: { title: "first" },
      },
    ]);

    const updateResult = await rowStore.txn([
      {
        kind: "put",
        collection: "highlights",
        id: "h-1",
        value: { title: "updated" },
      },
    ]);

    expect(updateResult.writes).toEqual([
      {
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        hlc: "1000-1-deviceA",
        tombstone: false,
      },
    ]);

    const readResult = await rowStore.txn([
      {
        kind: "get",
        collection: "highlights",
        id: "h-1",
      },
    ]);

    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: {
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        value: { title: "updated" },
        hlc: "1000-1-deviceA",
        txID: "tx_2",
        tombstone: false,
      },
    });
  });

  test("coalesces invalidation hints and notifies subscribers only for writes", async () => {
    const { rowStore } = createTestRowStore();
    const notifications: string[] = [];

    const unsubscribe = rowStore.subscribe((result) => {
      notifications.push(
        result.invalidationHints
          .map((hint) => `${hint.collection}:${hint.id ?? ""}:${hint.parentID ?? ""}`)
          .join("|"),
      );
    });

    await rowStore.txn([{ kind: "get_all", collection: "books" }]);
    expect(notifications).toEqual([]);

    await rowStore.txn([
      { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
      { kind: "put", collection: "books", id: "book-1", value: { title: "Dune 2" } },
    ]);

    expect(notifications).toEqual(["books:book-1:"]);

    unsubscribe();
    await rowStore.txn([
      { kind: "put", collection: "books", id: "book-2", value: { title: "Children of Dune" } },
    ]);

    expect(notifications).toEqual(["books:book-1:"]);
  });

  test("skips stale puts when existing row has newer HLC", async () => {
    const existingRow: StoredRow<BookValue> = {
      collection: "books",
      id: "book-1",
      parentID: null,
      value: { title: "Remote winner" },
      hlc: asClock("9000-0-deviceZ"),
      txID: "tx_remote",
      tombstone: false,
    };
    const { rowStore } = createTestRowStore({
      seedRows: [existingRow],
    });

    const writeResult = await rowStore.txn([
      {
        kind: "put",
        collection: "books",
        id: "book-1",
        value: { title: "Local stale write" },
      },
    ]);

    expect(writeResult.writes).toEqual([]);
    expect(writeResult.invalidationHints).toEqual([]);

    const readResult = await rowStore.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: existingRow,
    });
  });

  test("skips stale deletes when existing row has newer HLC", async () => {
    const existingRow: StoredRow<BookValue> = {
      collection: "books",
      id: "book-1",
      parentID: null,
      value: { title: "Remote winner" },
      hlc: asClock("9000-0-deviceZ"),
      txID: "tx_remote",
      tombstone: false,
    };
    const { rowStore } = createTestRowStore({
      seedRows: [existingRow],
    });

    const deleteResult = await rowStore.txn([
      { kind: "delete", collection: "books", id: "book-1" },
    ]);

    expect(deleteResult.writes).toEqual([]);
    expect(deleteResult.invalidationHints).toEqual([]);

    const readResult = await rowStore.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: existingRow,
    });
  });
});
