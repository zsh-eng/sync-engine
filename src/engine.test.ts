import { describe, expect, test } from "bun:test";

import { createEngine } from "./engine";
import { createClockService, type ClockStorageAdapter, type HybridLogicalClock } from "./hlc";
import { createInMemoryRowStoreAdapter } from "./row-store/in-memory-adapter";
import type { RowStoreOperation, StoredRow } from "./row-store/types";

interface BookValue {
  title: string;
}

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function createTestEngine(input: { seedRows?: ReadonlyArray<StoredRow<BookValue>> } = {}) {
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
  const engine = createEngine<BookValue>({
    adapter,
    clock,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return { engine, adapter };
}

function getResultAt(
  result: Awaited<ReturnType<ReturnType<typeof createTestEngine>["engine"]["txn"]>>,
  index: number,
) {
  const readResult = result.readResults[index];
  if (!readResult) {
    throw new Error(`Missing read result at index ${index}`);
  }
  return readResult;
}

describe("Engine core behavior", () => {
  test("assigns HLC values to writes and returns reads in op order", async () => {
    const { engine } = createTestEngine();

    const writeResult = await engine.txn([
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

    const readResult = await engine.txn([
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
    const { engine } = createTestEngine();

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

    await engine.txn(setupOps);

    const deleteResult = await engine.txn([
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

    const postDelete = await engine.txn([
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
    const { engine } = createTestEngine();

    await engine.txn([
      {
        kind: "put",
        collection: "highlights",
        id: "h-1",
        parentID: "book-1",
        value: { title: "first" },
      },
    ]);

    const updateResult = await engine.txn([
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

    const readResult = await engine.txn([
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
    const { engine } = createTestEngine();
    const notifications: string[] = [];

    const unsubscribe = engine.subscribe((result) => {
      notifications.push(
        result.invalidationHints
          .map((hint) => `${hint.collection}:${hint.id ?? ""}:${hint.parentID ?? ""}`)
          .join("|"),
      );
    });

    await engine.txn([{ kind: "get_all", collection: "books" }]);
    expect(notifications).toEqual([]);

    await engine.txn([
      { kind: "put", collection: "books", id: "book-1", value: { title: "Dune" } },
      { kind: "put", collection: "books", id: "book-1", value: { title: "Dune 2" } },
    ]);

    expect(notifications).toEqual(["books:book-1:"]);

    unsubscribe();
    await engine.txn([
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
    const { engine } = createTestEngine({
      seedRows: [existingRow],
    });

    const writeResult = await engine.txn([
      {
        kind: "put",
        collection: "books",
        id: "book-1",
        value: { title: "Local stale write" },
      },
    ]);

    expect(writeResult.writes).toEqual([]);
    expect(writeResult.invalidationHints).toEqual([]);

    const readResult = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
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
    const { engine } = createTestEngine({
      seedRows: [existingRow],
    });

    const deleteResult = await engine.txn([
      { kind: "delete", collection: "books", id: "book-1" },
    ]);

    expect(deleteResult.writes).toEqual([]);
    expect(deleteResult.invalidationHints).toEqual([]);

    const readResult = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: existingRow,
    });
  });
});

describe("Engine applyRemote", () => {
  test("applies remote rows with newer HLC", async () => {
    const { engine } = createTestEngine();

    await engine.txn([
      { kind: "put", collection: "books", id: "book-1", value: { title: "Local" } },
    ]);

    const result = await engine.applyRemote([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Remote update" },
        hlc: asClock("5000-0-deviceB"),
        txID: "tx_remote",
        tombstone: false,
      },
    ]);

    expect(result.appliedCount).toBe(1);
    expect(result.invalidationHints).toEqual([{ collection: "books", id: "book-1" }]);

    const readResult = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Remote update" },
        hlc: "5000-0-deviceB",
        txID: "tx_remote",
        tombstone: false,
      },
    });
  });

  test("rejects remote rows with older HLC", async () => {
    const { engine } = createTestEngine();

    await engine.applyRemote([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Newer remote" },
        hlc: asClock("9000-0-deviceZ"),
        txID: "tx_remote_1",
        tombstone: false,
      },
    ]);

    const result = await engine.applyRemote([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Older remote" },
        hlc: asClock("1000-0-deviceA"),
        txID: "tx_remote_2",
        tombstone: false,
      },
    ]);

    expect(result.appliedCount).toBe(0);
    expect(result.invalidationHints).toEqual([]);

    const readResult = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Newer remote" },
        hlc: "9000-0-deviceZ",
        txID: "tx_remote_1",
        tombstone: false,
      },
    });
  });

  test("applies remote tombstones", async () => {
    const { engine } = createTestEngine();

    await engine.txn([
      { kind: "put", collection: "books", id: "book-1", value: { title: "Local" } },
    ]);

    const result = await engine.applyRemote([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: null,
        hlc: asClock("5000-0-deviceB"),
        txID: "tx_remote",
        tombstone: true,
      },
    ]);

    expect(result.appliedCount).toBe(1);

    const readResult = await engine.txn([{ kind: "get", collection: "books", id: "book-1" }]);
    expect(getResultAt(readResult, 0)).toEqual({
      opIndex: 0,
      kind: "get",
      row: undefined,
    });
  });

  test("notifies subscribers on remote apply", async () => {
    const { engine } = createTestEngine();
    const notifications: string[] = [];

    engine.subscribe((result) => {
      notifications.push(
        result.invalidationHints
          .map((hint) => `${hint.collection}:${hint.id ?? ""}:${hint.parentID ?? ""}`)
          .join("|"),
      );
    });

    await engine.applyRemote([
      {
        collection: "books",
        id: "book-1",
        parentID: null,
        value: { title: "Remote" },
        hlc: asClock("5000-0-deviceB"),
        txID: "tx_remote",
        tombstone: false,
      },
    ]);

    expect(notifications).toEqual(["books:book-1:"]);
  });

  test("does not notify subscribers when no rows are applied", async () => {
    const { engine } = createTestEngine();
    const notifications: string[] = [];

    engine.subscribe((result) => {
      notifications.push("notified");
    });

    await engine.applyRemote([]);
    expect(notifications).toEqual([]);
  });
});
