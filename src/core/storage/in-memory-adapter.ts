import type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  PendingSequence,
  RowApplyOutcome,
  RowQuery,
  RowStorageAdapter,
  StoredRow,
} from "../types";
import {
  appendPendingOperations,
  compareHlc,
  getPendingOperations,
  removePendingOperationsThrough,
  rowKey,
} from "./shared";

function cloneRow<S extends CollectionValueMap>(row: AnyStoredRow<S>): AnyStoredRow<S> {
  return {
    ...row,
    data: row.data === null ? null : structuredClone(row.data),
  };
}

function cloneRowsMap<S extends CollectionValueMap>(
  rows: ReadonlyMap<string, AnyStoredRow<S>>,
): Map<string, AnyStoredRow<S>> {
  const next = new Map<string, AnyStoredRow<S>>();
  for (const [key, row] of rows.entries()) {
    next.set(key, cloneRow(row));
  }
  return next;
}

export interface CreateInMemoryRowStorageAdapterInput<S extends CollectionValueMap> {
  namespace?: string;
  seedRows?: ReadonlyArray<AnyStoredRow<S>>;
}

export class InMemoryRowStorageAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
> implements RowStorageAdapter<S> {
  private readonly namespace: string;
  private rows = new Map<string, AnyStoredRow<S>>();
  private pendingOperations: PendingOperation<S>[] = [];
  private readonly kvStore = new Map<string, unknown>();

  constructor(input: CreateInMemoryRowStorageAdapterInput<S> = {}) {
    this.namespace = input.namespace ?? "default";

    if (!input.seedRows) {
      return;
    }

    for (const row of input.seedRows) {
      if (row.namespace !== this.namespace) {
        continue;
      }

      this.rows.set(rowKey(row.collectionId, row.id), cloneRow(row));
    }
  }

  async query<C extends CollectionId<S>>(query: RowQuery<S, C>): Promise<Array<StoredRow<S, C>>> {
    const rows: StoredRow<S, C>[] = [];

    for (const row of this.rows.values()) {
      if (row.collectionId !== query.collectionId) {
        continue;
      }

      if (query.id !== undefined && row.id !== query.id) {
        continue;
      }

      if (query.parentId !== undefined && row.parentId !== query.parentId) {
        continue;
      }

      if (!query.includeTombstones && row.tombstone) {
        continue;
      }

      rows.push(cloneRow(row) as StoredRow<S, C>);
    }

    return rows;
  }

  async applyRows(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<Array<RowApplyOutcome<S>>> {
    const workingRows = cloneRowsMap(this.rows);
    const outcomes: RowApplyOutcome<S>[] = [];

    for (const row of rows) {
      if (row.namespace !== this.namespace) {
        throw new Error(
          `Namespace mismatch: adapter namespace is "${this.namespace}" but received "${row.namespace}"`,
        );
      }

      const key = rowKey(row.collectionId, row.id);
      const existing = workingRows.get(key);
      const written = !existing || compareHlc(row, existing) === 1;

      if (written) {
        workingRows.set(key, cloneRow(row));
      }

      outcomes.push({
        written,
        collectionId: row.collectionId,
        id: row.id,
        parentId: row.parentId,
        tombstone: row.tombstone,
        committedTimestampMs: row.committedTimestampMs,
        hlcTimestampMs: row.hlcTimestampMs,
        hlcCounter: row.hlcCounter,
        hlcDeviceId: row.hlcDeviceId,
      });
    }

    this.rows = workingRows;
    return outcomes;
  }

  async appendPending(operations: ReadonlyArray<PendingOperation<S>>): Promise<void> {
    appendPendingOperations(this.pendingOperations, operations);
  }

  async getPending(limit: number): Promise<Array<PendingOperation<S>>> {
    return getPendingOperations(this.pendingOperations, limit);
  }

  async removePendingThrough(sequenceInclusive: PendingSequence): Promise<void> {
    this.pendingOperations = removePendingOperationsThrough(
      this.pendingOperations,
      sequenceInclusive,
    );
  }

  async putKV(key: string, value: unknown): Promise<void> {
    this.kvStore.set(key, structuredClone(value));
  }

  async getKV<Value = unknown>(key: string): Promise<Value | undefined> {
    const value = this.kvStore.get(key);
    return value === undefined ? undefined : (structuredClone(value) as Value);
  }

  async deleteKV(key: string): Promise<void> {
    this.kvStore.delete(key);
  }

  async dumpAll(): Promise<Array<AnyStoredRow<S>>> {
    return [...this.rows.values()].map((row) => cloneRow(row));
  }
}

export function createInMemoryRowStorageAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
>(input: CreateInMemoryRowStorageAdapterInput<S> = {}): InMemoryRowStorageAdapter<S> {
  return new InMemoryRowStorageAdapter<S>(input);
}
