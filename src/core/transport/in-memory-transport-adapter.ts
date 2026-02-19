import type {
  AnyStoredRow,
  CollectionValueMap,
  SyncCursor,
  TransportAdapter,
  TransportEvent,
  TransportPullRequest,
  TransportPullResponse,
  TransportPushRequest,
  TransportPushResponse,
} from "../types";
import { compareHlc } from "../storage/shared";

const DEFAULT_NAMESPACE = "default";

function rowKey(collectionId: string, id: string): string {
  return `${collectionId}::${id}`;
}

function cloneRow<S extends CollectionValueMap>(row: AnyStoredRow<S>): AnyStoredRow<S> {
  return {
    ...row,
    data: row.data === null ? null : structuredClone(row.data),
  };
}

function compareRowOrder<S extends CollectionValueMap>(
  a: AnyStoredRow<S>,
  b: AnyStoredRow<S>,
): number {
  if (a.committedTimestampMs !== b.committedTimestampMs) {
    return a.committedTimestampMs - b.committedTimestampMs;
  }

  if (a.collectionId !== b.collectionId) {
    return a.collectionId < b.collectionId ? -1 : 1;
  }

  if (a.id === b.id) {
    return 0;
  }

  return a.id < b.id ? -1 : 1;
}

function compareRowToCursor<S extends CollectionValueMap>(
  row: AnyStoredRow<S>,
  cursor: SyncCursor,
): number {
  if (row.committedTimestampMs !== cursor.committedTimestampMs) {
    return row.committedTimestampMs - cursor.committedTimestampMs;
  }

  if (row.collectionId !== cursor.collectionId) {
    return row.collectionId < cursor.collectionId ? -1 : 1;
  }

  if (row.id === cursor.id) {
    return 0;
  }

  return row.id < cursor.id ? -1 : 1;
}

function toCursor<S extends CollectionValueMap>(row: AnyStoredRow<S>): SyncCursor {
  return {
    committedTimestampMs: row.committedTimestampMs,
    collectionId: row.collectionId,
    id: row.id,
  };
}

function emitEvent<S extends CollectionValueMap>(
  listeners: ReadonlySet<(event: TransportEvent<S>) => void>,
  event: TransportEvent<S>,
): void {
  for (const listener of listeners) {
    listener(event);
  }
}

function authError(): Error {
  return new Error("Authentication required for in-memory transport");
}

export interface CreateInMemoryTransportAdapterInput<S extends CollectionValueMap> {
  namespace?: string;
  seedRows?: ReadonlyArray<AnyStoredRow<S>>;
  now?: () => number;
}

export class InMemoryTransportAdapter<S extends CollectionValueMap> implements TransportAdapter<S> {
  private readonly namespace: string;
  private readonly now: () => number;
  private readonly listeners = new Set<(event: TransportEvent<S>) => void>();
  private readonly rows = new Map<string, AnyStoredRow<S>>();
  private requiresAuth = false;
  private lastCommittedTimestampMs = 0;

  constructor(input: CreateInMemoryTransportAdapterInput<S> = {}) {
    this.namespace = input.namespace ?? DEFAULT_NAMESPACE;
    this.now = input.now ?? Date.now;

    for (const row of input.seedRows ?? []) {
      if (row.namespace !== this.namespace) {
        continue;
      }

      this.rows.set(rowKey(row.collectionId, row.id), cloneRow(row));
      this.lastCommittedTimestampMs = Math.max(
        this.lastCommittedTimestampMs,
        row.committedTimestampMs,
      );
    }
  }

  setRequireAuth(required: boolean): void {
    this.requiresAuth = required;
  }

  async injectRemoteChanges(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<void> {
    const applied: AnyStoredRow<S>[] = [];

    for (const row of rows) {
      if (row.namespace !== this.namespace) {
        throw new Error(
          `Namespace mismatch: adapter namespace is "${this.namespace}" but received "${row.namespace}"`,
        );
      }

      const key = rowKey(row.collectionId, row.id);
      const existing = this.rows.get(key);
      const written = !existing || compareHlc(row, existing) === 1;
      if (!written) {
        continue;
      }

      const cloned = cloneRow(row);
      this.rows.set(key, cloned);
      applied.push(cloned);
      this.lastCommittedTimestampMs = Math.max(
        this.lastCommittedTimestampMs,
        row.committedTimestampMs,
      );
    }

    if (applied.length > 0) {
      emitEvent(this.listeners, {
        type: "serverChanges",
        changes: applied.map((row) => cloneRow(row)),
      });
    }
  }

  async snapshotRows(): Promise<Array<AnyStoredRow<S>>> {
    return [...this.rows.values()].map((row) => cloneRow(row)).sort(compareRowOrder);
  }

  onEvent(listener: (event: TransportEvent<S>) => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  async pull(request: TransportPullRequest<S>): Promise<TransportPullResponse<S>> {
    this.assertAuthorized();

    const rows = [...this.rows.values()]
      .filter((row) => {
        if (request.collectionId && row.collectionId !== request.collectionId) {
          return false;
        }
        if (request.parentId !== undefined && row.parentId !== request.parentId) {
          return false;
        }
        if (request.cursor && compareRowToCursor(row, request.cursor) <= 0) {
          return false;
        }
        return true;
      })
      .sort(compareRowOrder);

    const limit = Math.max(0, request.limit);
    const page = rows.slice(0, limit).map((row) => cloneRow(row));
    const hasMore = rows.length > page.length;
    const nextCursor = page.length > 0 ? toCursor(page[page.length - 1]!) : request.cursor;

    return {
      changes: page,
      nextCursor,
      hasMore,
    };
  }

  async push(request: TransportPushRequest<S>): Promise<TransportPushResponse> {
    this.assertAuthorized();

    if (request.operations.length === 0) {
      return {};
    }

    let acknowledgedThroughSequence = request.operations[0]!.sequence;

    for (const operation of request.operations) {
      acknowledgedThroughSequence = Math.max(acknowledgedThroughSequence, operation.sequence);

      const nextCommittedTimestampMs = this.nextCommittedTimestampMs();
      const row: AnyStoredRow<S> =
        operation.type === "put"
          ? {
              namespace: this.namespace,
              collectionId: operation.collectionId,
              id: operation.id,
              parentId: operation.parentId,
              data: structuredClone(operation.data),
              tombstone: false,
              txId: operation.txId,
              schemaVersion: operation.schemaVersion,
              committedTimestampMs: nextCommittedTimestampMs,
              hlcTimestampMs: operation.hlcTimestampMs,
              hlcCounter: operation.hlcCounter,
              hlcDeviceId: operation.hlcDeviceId,
            }
          : {
              namespace: this.namespace,
              collectionId: operation.collectionId,
              id: operation.id,
              parentId: operation.parentId,
              data: null,
              tombstone: true,
              txId: operation.txId,
              schemaVersion: operation.schemaVersion,
              committedTimestampMs: nextCommittedTimestampMs,
              hlcTimestampMs: operation.hlcTimestampMs,
              hlcCounter: operation.hlcCounter,
              hlcDeviceId: operation.hlcDeviceId,
            };

      const key = rowKey(row.collectionId, row.id);
      const existing = this.rows.get(key);
      const written = !existing || compareHlc(row, existing) === 1;
      if (written) {
        this.rows.set(key, cloneRow(row));
      }
    }

    return {
      acknowledgedThroughSequence,
    };
  }

  private assertAuthorized(): void {
    if (!this.requiresAuth) {
      return;
    }

    emitEvent(this.listeners, { type: "needsAuth" });
    throw authError();
  }

  private nextCommittedTimestampMs(): number {
    const nowMs = Math.floor(this.now());
    const next = Math.max(this.lastCommittedTimestampMs + 1, nowMs);
    this.lastCommittedTimestampMs = next;
    return next;
  }
}

export function createInMemoryTransportAdapter<S extends CollectionValueMap>(
  input: CreateInMemoryTransportAdapterInput<S> = {},
): InMemoryTransportAdapter<S> {
  return new InMemoryTransportAdapter<S>(input);
}
