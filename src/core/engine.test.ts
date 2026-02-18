import { describe, expect, test } from "bun:test";

import { createEngine } from "./engine";
import {
  createClockService,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "./hlc";
import { createInMemoryRowStorageAdapter } from "./storage/in-memory-adapter";
import type { AnyStoredRow } from "./types";

interface BookValue {
  title: string;
}

interface Collections {
  books: BookValue;
  highlights: BookValue;
}

const TEST_NAMESPACE = "books-app";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function remoteRow(
  input: Omit<
    AnyStoredRow<Collections>,
    "hlcTimestampMs" | "hlcCounter" | "hlcDeviceId" | "committedTimestampMs"
  > & {
    clock: HybridLogicalClock;
  },
): AnyStoredRow<Collections> {
  const parsed = parseClock(input.clock);
  return {
    namespace: input.namespace,
    collectionId: input.collectionId,
    id: input.id,
    parentId: input.parentId,
    data: input.data,
    txId: input.txId,
    tombstone: input.tombstone,
    committedTimestampMs: parsed.wallMs,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  };
}

function createTestEngine(input: { seedRows?: ReadonlyArray<AnyStoredRow<Collections>> } = {}) {
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
    now: () => 1_000,
  });

  const adapter = createInMemoryRowStorageAdapter<Collections>({
    namespace: TEST_NAMESPACE,
    seedRows: input.seedRows,
  });
  const engine = createEngine<Collections>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return { engine, adapter };
}

describe("Engine core behavior", () => {
  test("executes put/get/getAll operations", async () => {
    const { engine } = createTestEngine();

    const write = await engine.execute([
      {
        type: "put",
        collectionId: "books",
        id: "book-1",
        data: { title: "Dune" },
      },
    ] as const);

    expect(write[0]).toEqual({
      collectionId: "books",
      id: "book-1",
      parentId: null,
      committedTimestampMs: 1000,
      hlcTimestampMs: 1000,
      hlcCounter: 0,
      hlcDeviceId: "deviceA",
      tombstone: false,
      applied: true,
    });

    const read = await engine.execute([
      { type: "get", collectionId: "books", id: "book-1" },
      { type: "getAll", collectionId: "books" },
    ] as const);

    expect(read[0]).toEqual({
      namespace: TEST_NAMESPACE,
      collectionId: "books",
      id: "book-1",
      parentId: null,
      data: { title: "Dune" },
      txId: "tx_1",
      tombstone: false,
      committedTimestampMs: 1000,
      hlcTimestampMs: 1000,
      hlcCounter: 0,
      hlcDeviceId: "deviceA",
    });
    expect(read[1]).toHaveLength(1);
  });

  test("delete and deleteAllWithParent create tombstones and hide rows from reads", async () => {
    const { engine } = createTestEngine();

    await engine.execute([
      { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
      {
        type: "put",
        collectionId: "highlights",
        id: "h-1",
        parentId: "book-1",
        data: { title: "first highlight" },
      },
      {
        type: "put",
        collectionId: "highlights",
        id: "h-2",
        parentId: "book-1",
        data: { title: "second highlight" },
      },
    ] as const);

    const deleted = await engine.execute([
      { type: "delete", collectionId: "books", id: "book-1" },
      { type: "deleteAllWithParent", collectionId: "highlights", parentId: "book-1" },
    ] as const);

    expect(deleted[0]?.tombstone).toBe(true);
    expect(deleted[1]).toHaveLength(2);
    expect(deleted[1].every((entry) => entry.tombstone)).toBe(true);

    const postDelete = await engine.execute([
      { type: "get", collectionId: "books", id: "book-1" },
      { type: "getAllWithParent", collectionId: "highlights", parentId: "book-1" },
      { type: "getAll", collectionId: "highlights" },
    ] as const);

    expect(postDelete[0]).toBeUndefined();
    expect(postDelete[1]).toEqual([]);
    expect(postDelete[2]).toEqual([]);
  });

  test("preserves parentId when put omits parentId", async () => {
    const { engine } = createTestEngine();

    await engine.execute([
      {
        type: "put",
        collectionId: "highlights",
        id: "h-1",
        parentId: "book-1",
        data: { title: "first" },
      },
    ] as const);

    const update = await engine.execute([
      {
        type: "put",
        collectionId: "highlights",
        id: "h-1",
        data: { title: "updated" },
      },
    ] as const);

    expect(update[0]?.parentId).toBe("book-1");

    const read = await engine.execute([
      {
        type: "get",
        collectionId: "highlights",
        id: "h-1",
      },
    ] as const);

    expect(read[0]).toMatchObject({
      collectionId: "highlights",
      id: "h-1",
      parentId: "book-1",
      data: { title: "updated" },
      txId: "tx_2",
      tombstone: false,
    });
  });

  test("skips stale local writes when existing row has newer HLC", async () => {
    const existing = remoteRow({
      namespace: TEST_NAMESPACE,
      collectionId: "books",
      id: "book-1",
      parentId: null,
      data: { title: "Remote winner" },
      txId: "tx_remote",
      tombstone: false,
      clock: asClock("9000-0-deviceZ"),
    });
    const { engine } = createTestEngine({ seedRows: [existing] });

    const write = await engine.execute([
      {
        type: "put",
        collectionId: "books",
        id: "book-1",
        data: { title: "Local stale write" },
      },
    ] as const);

    expect(write[0]?.applied).toBe(false);

    const read = await engine.execute([
      { type: "get", collectionId: "books", id: "book-1" },
    ] as const);
    expect(read[0]).toMatchObject(existing);
  });
});

describe("Engine applyRemote", () => {
  test("applies newer remote rows and rejects older ones", async () => {
    const { engine } = createTestEngine();

    await engine.execute([
      { type: "put", collectionId: "books", id: "book-1", data: { title: "Local" } },
    ] as const);

    const applied = await engine.applyRemote([
      remoteRow({
        namespace: TEST_NAMESPACE,
        collectionId: "books",
        id: "book-1",
        parentId: null,
        data: { title: "Remote update" },
        txId: "tx_remote_new",
        tombstone: false,
        clock: asClock("5000-0-deviceB"),
      }),
    ]);
    expect(applied.appliedCount).toBe(1);

    const stale = await engine.applyRemote([
      remoteRow({
        namespace: TEST_NAMESPACE,
        collectionId: "books",
        id: "book-1",
        parentId: null,
        data: { title: "Remote stale" },
        txId: "tx_remote_old",
        tombstone: false,
        clock: asClock("1000-0-deviceA"),
      }),
    ]);
    expect(stale.appliedCount).toBe(0);

    const read = await engine.execute([
      { type: "get", collectionId: "books", id: "book-1" },
    ] as const);
    expect(read[0]).toMatchObject({
      data: { title: "Remote update" },
      txId: "tx_remote_new",
    });
  });

  test("tracks pending operations with sequence-based ack", async () => {
    const { engine } = createTestEngine();

    await engine.execute([
      { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
      { type: "put", collectionId: "books", id: "book-2", data: { title: "Messiah" } },
      { type: "delete", collectionId: "books", id: "book-1" },
    ] as const);

    const pending = await engine.getPending(10);
    expect(pending).toHaveLength(3);
    expect(pending.map((operation) => operation.sequence)).toEqual([1, 2, 3]);

    await engine.removePendingThrough(2);
    const remaining = await engine.getPending(10);
    expect(remaining.map((operation) => operation.sequence)).toEqual([3]);
  });

  test("notifies subscribers only when rows are written", async () => {
    const { engine } = createTestEngine();
    const events: string[] = [];

    engine.subscribe((event) => {
      events.push(
        `${event.source}:${event.invalidationHints
          .map((hint) => `${hint.collectionId}:${hint.id ?? ""}:${hint.parentId ?? ""}`)
          .join("|")}`,
      );
    });

    await engine.execute([{ type: "getAll", collectionId: "books" }] as const);
    expect(events).toEqual([]);

    await engine.execute([
      { type: "put", collectionId: "books", id: "book-1", data: { title: "A" } },
      { type: "put", collectionId: "books", id: "book-1", data: { title: "B" } },
    ] as const);

    await engine.applyRemote([
      remoteRow({
        namespace: TEST_NAMESPACE,
        collectionId: "books",
        id: "book-2",
        parentId: null,
        data: { title: "Remote" },
        txId: "tx_remote",
        tombstone: false,
        clock: asClock("5000-0-deviceB"),
      }),
    ]);

    expect(events).toEqual(["local:books:book-1:", "remote:books:book-2:"]);
  });
});
