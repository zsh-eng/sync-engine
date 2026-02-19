import { createSerialQueue } from "./internal/serial-queue";
import type {
  CollectionValueMap,
  ConnectionManager,
  ConnectionState,
  SyncCursor,
  SyncEngine,
  TransportAdapter,
  TransportEvent,
  Storage,
} from "./types";

const DEFAULT_CURSOR_KEY = "sync.cursor.v1";
const DEFAULT_INTERVAL_MS = 2_000;
const DEFAULT_PUSH_BATCH_SIZE = 100;
const DEFAULT_PULL_LIMIT = 100;

function assertPositiveInteger(value: number, field: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`Invalid ${field}: expected a positive integer, received ${value}`);
  }

  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isSyncCursor(value: unknown): value is SyncCursor {
  if (!isRecord(value)) {
    return false;
  }

  return (
    typeof value.committedTimestampMs === "number" &&
    Number.isFinite(value.committedTimestampMs) &&
    typeof value.collectionId === "string" &&
    typeof value.id === "string"
  );
}

function cursorsEqual(a: SyncCursor | undefined, b: SyncCursor | undefined): boolean {
  if (!a && !b) {
    return true;
  }

  if (!a || !b) {
    return false;
  }

  return (
    a.committedTimestampMs === b.committedTimestampMs &&
    a.collectionId === b.collectionId &&
    a.id === b.id
  );
}

function authRequiredError(): Error {
  return new Error("Transport adapter emitted needsAuth");
}

export interface CreateSyncEngineInput<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
> {
  storage: Storage<S, KV>;
  connectionManager: ConnectionManager;
  transportAdapter: TransportAdapter<S>;
  cursorKey?: string;
  intervalMs?: number;
  pushBatchSize?: number;
  pullLimit?: number;
  onError?: (error: unknown) => void;
}

export function createSyncEngine<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
>(input: CreateSyncEngineInput<S, KV>): SyncEngine {
  const queue = createSerialQueue();
  const cursorKey = input.cursorKey ?? DEFAULT_CURSOR_KEY;
  const intervalMs = assertPositiveInteger(input.intervalMs ?? DEFAULT_INTERVAL_MS, "intervalMs");
  const pushBatchSize = assertPositiveInteger(
    input.pushBatchSize ?? DEFAULT_PUSH_BATCH_SIZE,
    "pushBatchSize",
  );
  const pullLimit = assertPositiveInteger(input.pullLimit ?? DEFAULT_PULL_LIMIT, "pullLimit");

  let started = false;
  let syncQueued = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let unsubscribeConnection: (() => void) | undefined;
  let unsubscribeTransport: (() => void) | undefined;

  const reportError = (error: unknown): void => {
    if (!input.onError) {
      return;
    }

    input.onError(error);
  };

  const isConnected = (): boolean => input.connectionManager.getState() === "connected";

  const clearTimer = (): void => {
    if (timer === undefined) {
      return;
    }

    clearTimeout(timer);
    timer = undefined;
  };

  const readCursor = async (): Promise<SyncCursor | undefined> => {
    const value = await input.storage.getKV(cursorKey as keyof KV & string);
    if (value === undefined) {
      return undefined;
    }

    if (!isSyncCursor(value)) {
      reportError(new Error(`Invalid sync cursor at key "${cursorKey}"`));
      return undefined;
    }

    return value;
  };

  const persistCursor = async (cursor: SyncCursor): Promise<void> => {
    await input.storage.putKV(cursorKey as keyof KV & string, cursor as KV[keyof KV & string]);
  };

  const runPushPhase = async (): Promise<void> => {
    let lastFirstPendingSequence: number | undefined;

    while (started && isConnected()) {
      const pending = await input.storage.getPending(pushBatchSize);
      if (pending.length === 0) {
        return;
      }

      const firstSequence = pending[0]!.sequence;
      if (
        lastFirstPendingSequence !== undefined &&
        firstSequence <= lastFirstPendingSequence
      ) {
        return;
      }

      lastFirstPendingSequence = firstSequence;

      const response = await input.transportAdapter.push({ operations: pending });
      const acknowledgedThroughSequence = response.acknowledgedThroughSequence;
      if (
        acknowledgedThroughSequence === undefined ||
        acknowledgedThroughSequence < firstSequence
      ) {
        return;
      }

      await input.storage.removePendingThrough(acknowledgedThroughSequence);
    }
  };

  const runPullPhase = async (): Promise<void> => {
    let cursor = await readCursor();

    while (started && isConnected()) {
      const response = await input.transportAdapter.pull({ cursor, limit: pullLimit });

      if (response.changes.length > 0) {
        await input.storage.applyRemote(response.changes);
      }

      const nextCursor = response.nextCursor;
      const cursorDidAdvance = nextCursor !== undefined && !cursorsEqual(cursor, nextCursor);

      if (cursorDidAdvance) {
        await persistCursor(nextCursor);
        cursor = nextCursor;
      }

      if (!response.hasMore) {
        return;
      }

      if (!cursorDidAdvance) {
        return;
      }
    }
  };

  const runSyncCycle = async (): Promise<void> => {
    if (!started || !isConnected()) {
      return;
    }

    await runPushPhase();
    if (!started || !isConnected()) {
      return;
    }

    await runPullPhase();
  };

  const scheduleNextCycle = (): void => {
    if (!started || !isConnected()) {
      return;
    }

    clearTimer();
    timer = setTimeout(() => {
      timer = undefined;
      queueSyncCycle(false);
    }, intervalMs);
  };

  const queueSyncCycle = (clearExistingTimer: boolean): void => {
    if (!started || !isConnected()) {
      return;
    }

    if (clearExistingTimer) {
      clearTimer();
    }

    if (syncQueued) {
      return;
    }

    syncQueued = true;
    void queue.run(async () => {
      syncQueued = false;

      if (!started || !isConnected()) {
        return;
      }

      try {
        await runSyncCycle();
      } catch (error) {
        reportError(error);
      } finally {
        if (!started || !isConnected()) {
          return;
        }

        if (syncQueued) {
          return;
        }

        scheduleNextCycle();
      }
    });
  };

  const onConnectionStateChange = (state: ConnectionState): void => {
    if (!started) {
      return;
    }

    if (state !== "connected") {
      clearTimer();
      return;
    }

    queueSyncCycle(true);
  };

  const onTransportEvent = (event: TransportEvent<S>): void => {
    if (!started) {
      return;
    }

    if (event.type === "needsAuth") {
      reportError(authRequiredError());
      return;
    }

    void queue.run(async () => {
      if (!started) {
        return;
      }

      try {
        await input.storage.applyRemote(event.changes);
      } catch (error) {
        reportError(error);
      }
    });
  };

  return {
    async start(): Promise<void> {
      if (started) {
        return;
      }

      started = true;
      unsubscribeConnection = input.connectionManager.subscribe(onConnectionStateChange);
      unsubscribeTransport = input.transportAdapter.onEvent(onTransportEvent);

      if (isConnected()) {
        queueSyncCycle(true);
      }
    },

    async stop(): Promise<void> {
      if (!started) {
        return;
      }

      started = false;
      syncQueued = false;
      clearTimer();

      unsubscribeConnection?.();
      unsubscribeConnection = undefined;

      unsubscribeTransport?.();
      unsubscribeTransport = undefined;

      await queue.run(async () => undefined);
    },
  };
}
