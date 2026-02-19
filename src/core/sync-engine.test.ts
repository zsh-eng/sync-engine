import { describe, expect, test } from "bun:test";

import { createEngine } from "./engine";
import {
  createClockService,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "./hlc";
import { createInMemoryRowStorageAdapter } from "./storage/in-memory-adapter";
import { createSyncEngine } from "./sync-engine";
import type {
  AnyStoredRow,
  CollectionValueMap,
  ConnectionManager,
  ConnectionState,
  SyncCursor,
  TransportAdapter,
  TransportEvent,
  TransportPullRequest,
  TransportPullResponse,
  TransportPushRequest,
  TransportPushResponse,
} from "./types";

interface BookValue {
  title: string;
}

interface Collections {
  books: BookValue;
}

const TEST_NAMESPACE = "books-app";
const DEFAULT_CURSOR_KEY = "sync.cursor.v1";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function makeRemoteRow(input: {
  id: string;
  title: string;
  committedTimestampMs: number;
  clock: HybridLogicalClock;
}): AnyStoredRow<Collections> {
  const parsed = parseClock(input.clock);

  return {
    namespace: TEST_NAMESPACE,
    collectionId: "books",
    id: input.id,
    parentId: null,
    data: { title: input.title },
    tombstone: false,
    committedTimestampMs: input.committedTimestampMs,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  };
}

function createStorage() {
  let storedClock: HybridLogicalClock | undefined;
  let txCounter = 0;
  let nowMs = 1_000;

  const clockStorage: ClockStorageAdapter = {
    read: () => storedClock,
    write: (clock) => {
      storedClock = clock;
    },
  };

  const clock = createClockService({
    nodeId: "deviceA",
    storage: clockStorage,
    now: () => ++nowMs,
  });
  const adapter = createInMemoryRowStorageAdapter<Collections>({
    namespace: TEST_NAMESPACE,
  });

  return createEngine<Collections, Record<string, unknown>>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
    txIDFactory: () => `tx_${++txCounter}`,
  });
}

function createConnectionHarness(initialState: ConnectionState = "offline"): {
  manager: ConnectionManager;
  setState: (state: ConnectionState) => void;
  listenerCount: () => number;
} {
  let state = initialState;
  const listeners = new Set<(state: ConnectionState) => void>();

  return {
    manager: {
      getState: () => state,
      subscribe(listener: (nextState: ConnectionState) => void): () => void {
        listeners.add(listener);
        return () => {
          listeners.delete(listener);
        };
      },
    },
    setState(nextState: ConnectionState): void {
      if (nextState === state) {
        return;
      }

      state = nextState;
      for (const listener of listeners) {
        listener(nextState);
      }
    },
    listenerCount: () => listeners.size,
  };
}

type PullImpl<S extends CollectionValueMap> = (
  request: TransportPullRequest<S>,
) => Promise<TransportPullResponse<S>>;
type PushImpl<S extends CollectionValueMap> = (
  request: TransportPushRequest<S>,
) => Promise<TransportPushResponse>;

function createTransportHarness(input: {
  pullImpl?: PullImpl<Collections>;
  pushImpl?: PushImpl<Collections>;
} = {}) {
  const listeners = new Set<(event: TransportEvent<Collections>) => void>();
  const pullCalls: TransportPullRequest<Collections>[] = [];
  const pushCalls: TransportPushRequest<Collections>[] = [];

  let pullImpl =
    input.pullImpl ??
    (async (): Promise<TransportPullResponse<Collections>> => {
      return {
        changes: [],
        hasMore: false,
      };
    });
  let pushImpl =
    input.pushImpl ??
    (async (
      request: TransportPushRequest<Collections>,
    ): Promise<TransportPushResponse> => {
      if (request.operations.length === 0) {
        return {};
      }

      let maxSequence = request.operations[0]!.sequence;
      for (let index = 1; index < request.operations.length; index += 1) {
        maxSequence = Math.max(maxSequence, request.operations[index]!.sequence);
      }

      return {
        acknowledgedThroughSequence: maxSequence,
      };
    });

  const adapter: TransportAdapter<Collections> = {
    async pull(request: TransportPullRequest<Collections>): Promise<TransportPullResponse<Collections>> {
      pullCalls.push(structuredClone(request));
      return pullImpl(request);
    },

    async push(request: TransportPushRequest<Collections>): Promise<TransportPushResponse> {
      pushCalls.push(structuredClone(request));
      return pushImpl(request);
    },

    onEvent(listener: (event: TransportEvent<Collections>) => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };

  return {
    adapter,
    pullCalls,
    pushCalls,
    setPullImpl(nextImpl: PullImpl<Collections>): void {
      pullImpl = nextImpl;
    },
    setPushImpl(nextImpl: PushImpl<Collections>): void {
      pushImpl = nextImpl;
    },
    emit(event: TransportEvent<Collections>): void {
      for (const listener of listeners) {
        listener(event);
      }
    },
    listenerCount: () => listeners.size,
  };
}

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 1_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() <= deadline) {
    if (await condition()) {
      return;
    }

    await Bun.sleep(5);
  }

  throw new Error("Timed out waiting for condition");
}

describe("createSyncEngine", () => {
  test("starts offline and waits until connected before syncing", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("offline");
    const transport = createTransportHarness();
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 40,
    });

    try {
      await sync.start();
      await Bun.sleep(30);
      expect(transport.pushCalls).toHaveLength(0);
      expect(transport.pullCalls).toHaveLength(0);

      connection.setState("connected");
      await waitFor(() => transport.pullCalls.length >= 1);
    } finally {
      await sync.stop();
    }
  });

  test("pushes pending operations and removes them through acknowledged sequence", async () => {
    const storage = createStorage();
    await storage.put("books", "book-1", { title: "Dune" });
    await storage.put("books", "book-2", { title: "Messiah" });
    expect((await storage.getPending(10)).map((op) => op.sequence)).toEqual([1, 2]);

    const connection = createConnectionHarness("offline");
    const transport = createTransportHarness();
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 40,
    });

    try {
      await sync.start();
      connection.setState("connected");

      await waitFor(() => transport.pushCalls.length >= 1);
      await waitFor(async () => (await storage.getPending(10)).length === 0);

      expect(transport.pushCalls[0]?.operations.map((op) => op.sequence)).toEqual([1, 2]);
    } finally {
      await sync.stop();
    }
  });

  test("pulls all pages, applies remote rows, and persists cursor", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("offline");
    const transport = createTransportHarness();

    const cursor1: SyncCursor = {
      committedTimestampMs: 50_000,
      collectionId: "books",
      id: "book-1",
    };
    const cursor2: SyncCursor = {
      committedTimestampMs: 50_001,
      collectionId: "books",
      id: "book-2",
    };
    const row1 = makeRemoteRow({
      id: "book-1",
      title: "Remote 1",
      committedTimestampMs: 50_000,
      clock: asClock("5000-0-deviceR"),
    });
    const row2 = makeRemoteRow({
      id: "book-2",
      title: "Remote 2",
      committedTimestampMs: 50_001,
      clock: asClock("5001-0-deviceR"),
    });

    let pullCount = 0;
    transport.setPullImpl(async () => {
      pullCount += 1;
      if (pullCount === 1) {
        return {
          changes: [row1],
          nextCursor: cursor1,
          hasMore: true,
        };
      }

      return {
        changes: [row2],
        nextCursor: cursor2,
        hasMore: false,
      };
    });

    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 60,
    });

    try {
      await sync.start();
      connection.setState("connected");

      await waitFor(() => transport.pullCalls.length >= 2);

      expect(await storage.get("books", "book-1")).toMatchObject({
        data: { title: "Remote 1" },
      });
      expect(await storage.get("books", "book-2")).toMatchObject({
        data: { title: "Remote 2" },
      });
      expect(await storage.getKV(DEFAULT_CURSOR_KEY)).toEqual(cursor2);
    } finally {
      await sync.stop();
    }
  });

  test("resumes from persisted cursor on later cycles", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("offline");
    const transport = createTransportHarness();

    const cursor1: SyncCursor = {
      committedTimestampMs: 60_000,
      collectionId: "books",
      id: "book-1",
    };
    const cursor2: SyncCursor = {
      committedTimestampMs: 60_001,
      collectionId: "books",
      id: "book-2",
    };
    const row1 = makeRemoteRow({
      id: "book-1",
      title: "Cursor page 1",
      committedTimestampMs: 60_000,
      clock: asClock("6000-0-deviceR"),
    });
    const row2 = makeRemoteRow({
      id: "book-2",
      title: "Cursor page 2",
      committedTimestampMs: 60_001,
      clock: asClock("6001-0-deviceR"),
    });

    let pullCount = 0;
    transport.setPullImpl(async (request) => {
      pullCount += 1;
      if (pullCount === 1) {
        expect(request.cursor).toBeUndefined();
        return {
          changes: [row1],
          nextCursor: cursor1,
          hasMore: true,
        };
      }

      if (pullCount === 2) {
        expect(request.cursor).toEqual(cursor1);
        return {
          changes: [row2],
          nextCursor: cursor2,
          hasMore: false,
        };
      }

      return {
        changes: [],
        nextCursor: request.cursor,
        hasMore: false,
      };
    });

    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 20,
    });

    try {
      await sync.start();
      connection.setState("connected");

      await waitFor(() => transport.pullCalls.length >= 3);
      expect(transport.pullCalls[2]?.cursor).toEqual(cursor2);
      expect(await storage.getKV(DEFAULT_CURSOR_KEY)).toEqual(cursor2);
    } finally {
      await sync.stop();
    }
  });

  test("stop clears timers and unsubscribes listeners", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("connected");
    const transport = createTransportHarness();
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 20,
    });

    await sync.start();
    await waitFor(() => transport.pullCalls.length >= 1);
    expect(connection.listenerCount()).toBe(1);
    expect(transport.listenerCount()).toBe(1);

    await sync.stop();
    const pullCallsAtStop = transport.pullCalls.length;
    await Bun.sleep(50);

    expect(transport.pullCalls.length).toBe(pullCallsAtStop);
    expect(connection.listenerCount()).toBe(0);
    expect(transport.listenerCount()).toBe(0);
  });

  test("does not overlap sync cycles when pull is slow", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("connected");
    const transport = createTransportHarness();

    let activePulls = 0;
    let maxActivePulls = 0;
    transport.setPullImpl(async () => {
      activePulls += 1;
      maxActivePulls = Math.max(maxActivePulls, activePulls);
      await Bun.sleep(30);
      activePulls -= 1;

      return {
        changes: [],
        hasMore: false,
      };
    });

    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 5,
    });

    try {
      await sync.start();
      await waitFor(() => transport.pullCalls.length >= 2);
      expect(maxActivePulls).toBe(1);
    } finally {
      await sync.stop();
    }
  });

  test("reports push errors and retries on next interval", async () => {
    const storage = createStorage();
    await storage.put("books", "book-1", { title: "Retry me" });

    const connection = createConnectionHarness("connected");
    const transport = createTransportHarness();
    const errors: unknown[] = [];

    let pushCount = 0;
    transport.setPushImpl(async (request) => {
      pushCount += 1;
      if (pushCount === 1) {
        throw new Error("push failed");
      }

      return {
        acknowledgedThroughSequence: request.operations[request.operations.length - 1]!.sequence,
      };
    });

    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 20,
      onError: (error) => {
        errors.push(error);
      },
    });

    try {
      await sync.start();
      await waitFor(() => transport.pushCalls.length >= 2);
      await waitFor(async () => (await storage.getPending(10)).length === 0);

      expect(errors.length).toBeGreaterThanOrEqual(1);
      expect((errors[0] as Error).message).toBe("push failed");
    } finally {
      await sync.stop();
    }
  });

  test("missing push ack exits push loop without spinning", async () => {
    const storage = createStorage();
    await storage.put("books", "book-1", { title: "No ack" });

    const connection = createConnectionHarness("connected");
    const transport = createTransportHarness({
      pushImpl: async () => ({}),
    });
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 200,
    });

    try {
      await sync.start();
      await waitFor(() => transport.pushCalls.length >= 1);
      await Bun.sleep(40);

      expect(transport.pushCalls.length).toBe(1);
      expect((await storage.getPending(10)).length).toBe(1);
    } finally {
      await sync.stop();
    }
  });

  test("invalid hasMore pull response exits loop when cursor does not advance", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("connected");
    const transport = createTransportHarness({
      pullImpl: async () => ({
        changes: [],
        hasMore: true,
      }),
    });
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 200,
    });

    try {
      await sync.start();
      await waitFor(() => transport.pullCalls.length >= 1);
      await Bun.sleep(40);

      expect(transport.pullCalls.length).toBe(1);
    } finally {
      await sync.stop();
    }
  });

  test("applies serverChanges transport events and reports needsAuth", async () => {
    const storage = createStorage();
    const connection = createConnectionHarness("offline");
    const transport = createTransportHarness();
    const errors: unknown[] = [];
    const sync = createSyncEngine({
      storage,
      connectionManager: connection.manager,
      transportAdapter: transport.adapter,
      intervalMs: 200,
      onError: (error) => {
        errors.push(error);
      },
    });

    const row = makeRemoteRow({
      id: "book-remote",
      title: "From event",
      committedTimestampMs: 70_000,
      clock: asClock("7000-0-deviceR"),
    });

    try {
      await sync.start();
      transport.emit({
        type: "serverChanges",
        changes: [row],
      });
      transport.emit({
        type: "needsAuth",
      });

      await waitFor(async () => {
        const read = await storage.get("books", "book-remote");
        return read?.data?.title === "From event";
      });
      await waitFor(() => errors.length >= 1);

      expect((errors[0] as Error).message).toBe("Transport adapter emitted needsAuth");
    } finally {
      await sync.stop();
    }
  });
});
