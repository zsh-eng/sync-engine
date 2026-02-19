import { describe, expect, test } from "bun:test";

import { createEngine } from "../engine";
import {
  createClockService,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "../hlc";
import { createInMemoryRowStorageAdapter } from "../storage/in-memory-adapter";
import type { AnyStoredRow, PendingOperation, SyncCursor } from "../types";
import { createInMemoryTransportAdapter } from "./in-memory-transport-adapter";

interface BookValue {
  title: string;
}

interface HighlightValue {
  text: string;
}

interface Collections {
  books: BookValue;
  highlights: HighlightValue;
}

const TEST_NAMESPACE = "books-app";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function makePutOperation<C extends "books" | "highlights">(input: {
  sequence: number;
  collectionId: C;
  id: string;
  data: Collections[C];
  parentId?: string | null;
  txId?: string;
  schemaVersion?: number;
  clock: HybridLogicalClock;
}): PendingOperation<Collections> {
  const parsed = parseClock(input.clock);
  return {
    sequence: input.sequence,
    type: "put",
    collectionId: input.collectionId,
    id: input.id,
    parentId: input.parentId ?? null,
    data: structuredClone(input.data),
    txId: input.txId,
    schemaVersion: input.schemaVersion,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  } as PendingOperation<Collections>;
}

function makeRemoteRow<C extends "books" | "highlights">(input: {
  collectionId: C;
  id: string;
  parentId?: string | null;
  data: Collections[C] | null;
  tombstone: boolean;
  txId?: string;
  schemaVersion?: number;
  committedTimestampMs: number;
  clock: HybridLogicalClock;
}): AnyStoredRow<Collections> {
  const parsed = parseClock(input.clock);
  return {
    namespace: TEST_NAMESPACE,
    collectionId: input.collectionId,
    id: input.id,
    parentId: input.parentId ?? null,
    data: input.data as AnyStoredRow<Collections>["data"],
    tombstone: input.tombstone,
    txId: input.txId,
    schemaVersion: input.schemaVersion,
    committedTimestampMs: input.committedTimestampMs,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  };
}

function createClientEngine(nodeId: string, nowMs: number) {
  let storedClock: HybridLogicalClock | undefined;

  const clockStorage: ClockStorageAdapter = {
    read: () => storedClock,
    write: (clock) => {
      storedClock = clock;
    },
  };

  const clock = createClockService({
    nodeId,
    storage: clockStorage,
    now: () => nowMs,
  });

  const adapter = createInMemoryRowStorageAdapter<Collections>({
    namespace: TEST_NAMESPACE,
  });

  let txCounter = 0;
  const engine = createEngine<Collections>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
    txIDFactory: () => `${nodeId}_tx_${++txCounter}`,
  });

  return { engine };
}

describe("InMemoryTransportAdapter", () => {
  test("push/pull supports deterministic ordering and cursor pagination", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
      now: () => 10_000,
    });

    const pushResponse = await transport.push({
      operations: [
        makePutOperation({
          sequence: 1,
          collectionId: "books",
          id: "book-1",
          data: { title: "Dune" },
          clock: asClock("1000-0-deviceA"),
        }),
        makePutOperation({
          sequence: 2,
          collectionId: "books",
          id: "book-2",
          data: { title: "Messiah" },
          clock: asClock("1000-1-deviceA"),
        }),
        makePutOperation({
          sequence: 3,
          collectionId: "highlights",
          id: "h-1",
          parentId: "book-1",
          data: { text: "Fear is the mind-killer" },
          clock: asClock("1000-2-deviceA"),
        }),
      ],
    });
    expect(pushResponse).toEqual({ acknowledgedThroughSequence: 3 });

    const firstPage = await transport.pull({ limit: 2 });
    expect(firstPage.changes.map((row) => `${row.collectionId}:${row.id}`)).toEqual([
      "books:book-1",
      "books:book-2",
    ]);
    expect(firstPage.hasMore).toBe(true);
    expect(firstPage.nextCursor).toEqual({
      committedTimestampMs: 10_001,
      collectionId: "books",
      id: "book-2",
    });

    const secondPage = await transport.pull({
      limit: 2,
      cursor: firstPage.nextCursor,
    });
    expect(secondPage.changes.map((row) => `${row.collectionId}:${row.id}`)).toEqual([
      "highlights:h-1",
    ]);
    expect(secondPage.hasMore).toBe(false);
    expect(secondPage.nextCursor).toEqual({
      committedTimestampMs: 10_002,
      collectionId: "highlights",
      id: "h-1",
    });
  });

  test("LWW rejects stale pushes and preserves winner", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
      now: () => 20_000,
    });

    await transport.push({
      operations: [
        makePutOperation({
          sequence: 1,
          collectionId: "books",
          id: "book-1",
          data: { title: "Winner" },
          clock: asClock("9000-0-deviceZ"),
        }),
      ],
    });
    await transport.push({
      operations: [
        makePutOperation({
          sequence: 2,
          collectionId: "books",
          id: "book-1",
          data: { title: "Stale" },
          clock: asClock("1000-0-deviceA"),
        }),
      ],
    });

    const snapshot = await transport.snapshotRows();
    expect(snapshot).toHaveLength(1);
    expect(snapshot[0]).toMatchObject({
      collectionId: "books",
      id: "book-1",
      data: { title: "Winner" },
      hlcTimestampMs: 9000,
      hlcCounter: 0,
      hlcDeviceId: "deviceZ",
    });
  });

  test("pull filters by collectionId and parentId", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
      now: () => 30_000,
    });

    await transport.push({
      operations: [
        makePutOperation({
          sequence: 1,
          collectionId: "books",
          id: "book-1",
          data: { title: "Dune" },
          clock: asClock("1000-0-deviceA"),
        }),
        makePutOperation({
          sequence: 2,
          collectionId: "highlights",
          id: "h-1",
          parentId: "book-1",
          data: { text: "h1" },
          clock: asClock("1000-1-deviceA"),
        }),
        makePutOperation({
          sequence: 3,
          collectionId: "highlights",
          id: "h-2",
          parentId: "book-2",
          data: { text: "h2" },
          clock: asClock("1000-2-deviceA"),
        }),
      ],
    });

    const filtered = await transport.pull({
      limit: 10,
      collectionId: "highlights",
      parentId: "book-1",
    });
    expect(filtered.changes).toHaveLength(1);
    expect(filtered.changes[0]).toMatchObject({
      collectionId: "highlights",
      id: "h-1",
      parentId: "book-1",
    });
  });

  test("acknowledges through max sequence in request", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
      now: () => 40_000,
    });

    const response = await transport.push({
      operations: [
        makePutOperation({
          sequence: 4,
          collectionId: "books",
          id: "book-1",
          data: { title: "A" },
          clock: asClock("1000-0-deviceA"),
        }),
        makePutOperation({
          sequence: 9,
          collectionId: "books",
          id: "book-2",
          data: { title: "B" },
          clock: asClock("1000-1-deviceA"),
        }),
        makePutOperation({
          sequence: 7,
          collectionId: "books",
          id: "book-3",
          data: { title: "C" },
          clock: asClock("1000-2-deviceA"),
        }),
      ],
    });

    expect(response).toEqual({ acknowledgedThroughSequence: 9 });
  });

  test("auth gate emits needsAuth and throws for pull/push", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
    });
    const events: string[] = [];

    transport.onEvent((event) => {
      events.push(event.type);
    });
    transport.setRequireAuth(true);

    await expect(transport.pull({ limit: 1 })).rejects.toThrow(
      "Authentication required for in-memory transport",
    );
    await expect(transport.push({ operations: [] })).rejects.toThrow(
      "Authentication required for in-memory transport",
    );

    expect(events).toEqual(["needsAuth", "needsAuth"]);
  });

  test("injectRemoteChanges emits serverChanges and updates pull results", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
    });
    const serverEvents: Array<Array<string>> = [];

    transport.onEvent((event) => {
      if (event.type !== "serverChanges") {
        return;
      }

      serverEvents.push(event.changes.map((row) => `${row.collectionId}:${row.id}`));
    });

    await transport.injectRemoteChanges([
      makeRemoteRow({
        collectionId: "books",
        id: "book-remote",
        data: { title: "Remote Book" },
        tombstone: false,
        committedTimestampMs: 50_000,
        clock: asClock("5000-0-deviceR"),
      }),
    ]);

    expect(serverEvents).toEqual([["books:book-remote"]]);

    const pulled = await transport.pull({ limit: 10 });
    expect(pulled.changes).toHaveLength(1);
    expect(pulled.changes[0]).toMatchObject({
      id: "book-remote",
      data: { title: "Remote Book" },
    });
  });

  test("integration flow: client A push replicates to client B via pull/apply", async () => {
    const transport = createInMemoryTransportAdapter<Collections>({
      namespace: TEST_NAMESPACE,
      now: (() => {
        let ts = 60_000;
        return () => ++ts;
      })(),
    });
    const clientA = createClientEngine("deviceA", 1_000);
    const clientB = createClientEngine("deviceB", 2_000);
    let cursor: SyncCursor | undefined;

    await clientA.engine.put("books", "book-1", { title: "Dune" });
    const pending = await clientA.engine.getPending(10);
    expect(pending.map((operation) => operation.sequence)).toEqual([1]);

    const pushResponse = await transport.push({ operations: pending });
    expect(pushResponse.acknowledgedThroughSequence).toBe(1);
    await clientA.engine.removePendingThrough(pushResponse.acknowledgedThroughSequence!);
    expect(await clientA.engine.getPending(10)).toEqual([]);

    const pullResponse = await transport.pull({ cursor, limit: 10 });
    cursor = pullResponse.nextCursor;
    expect(pullResponse.changes).toHaveLength(1);
    expect(cursor).toEqual({
      committedTimestampMs: 60_001,
      collectionId: "books",
      id: "book-1",
    });

    const applyResult = await clientB.engine.applyRemote(pullResponse.changes);
    expect(applyResult.appliedCount).toBe(1);

    const replicated = await clientB.engine.get("books", "book-1");
    expect(replicated).toMatchObject({
      namespace: TEST_NAMESPACE,
      collectionId: "books",
      id: "book-1",
      data: { title: "Dune" },
      tombstone: false,
    });
  });
});
