import Dexie, { type Table } from "dexie";

import type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  PendingSequence,
  RowApplyOutcome,
  RowId,
  RowStorageAdapter,
  StoredRow,
} from "../../core/types";
import {
  appendPendingOperations,
  compareHlc,
  getPendingOperations,
  removePendingOperationsThrough,
} from "../../core/storage/shared";

export interface IndexedDbRowRecord<S extends CollectionValueMap = Record<string, unknown>> {
  namespace: string;
  collectionId: CollectionId<S>;
  id: string;
  parentId: string | null;
  data: unknown | null;
  committedTimestampMs: number;
  hlcTimestampMs: number;
  hlcCounter: number;
  hlcDeviceId: string;
  txId: string | null;
  tombstone: 0 | 1;
}

class RowStoreDexieDatabase<S extends CollectionValueMap = Record<string, unknown>> extends Dexie {
  readonly rows!: Table<IndexedDbRowRecord<S>, [string, string, string]>;

  constructor(
    name: string,
    options?: {
      indexedDB?: IDBFactory;
      IDBKeyRange?: typeof IDBKeyRange;
    },
  ) {
    super(name, options);

    this.version(1).stores({
      rows: "&[namespace+collectionId+id], [namespace+collectionId], [namespace+collectionId+parentId], [namespace+collectionId+tombstone], [namespace+committedTimestampMs+collectionId+id], [namespace+hlcTimestampMs+hlcCounter+hlcDeviceId]",
    });
  }
}

function assertNamespace(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Invalid namespace: expected a non-empty string");
  }

  return value;
}

function toStoredRow<S extends CollectionValueMap>(row: IndexedDbRowRecord<S>): AnyStoredRow<S> {
  return {
    namespace: row.namespace,
    collectionId: row.collectionId,
    id: row.id,
    parentId: row.parentId,
    data: row.data as AnyStoredRow<S>["data"],
    tombstone: row.tombstone === 1,
    txId: row.txId ?? undefined,
    committedTimestampMs: row.committedTimestampMs,
    hlcTimestampMs: row.hlcTimestampMs,
    hlcCounter: row.hlcCounter,
    hlcDeviceId: row.hlcDeviceId,
  };
}

function toIndexedDbRow<S extends CollectionValueMap>(
  namespace: string,
  row: AnyStoredRow<S>,
): IndexedDbRowRecord<S> {
  return {
    namespace,
    collectionId: row.collectionId,
    id: row.id,
    parentId: row.parentId,
    data: row.data,
    committedTimestampMs: row.committedTimestampMs,
    hlcTimestampMs: row.hlcTimestampMs,
    hlcCounter: row.hlcCounter,
    hlcDeviceId: row.hlcDeviceId,
    txId: row.txId ?? null,
    tombstone: row.tombstone ? 1 : 0,
  };
}

export interface CreateIndexedDbRowStoreAdapterInput {
  dbName: string;
  namespace: string;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
}

export class IndexedDbRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
> implements RowStorageAdapter<S> {
  private readonly db: RowStoreDexieDatabase<S>;
  private readonly namespace: string;
  private pendingOperations: PendingOperation<S>[] = [];

  constructor(input: CreateIndexedDbRowStoreAdapterInput) {
    const hasCustomIndexedDb = input.indexedDB !== undefined || input.IDBKeyRange !== undefined;
    const options = hasCustomIndexedDb
      ? {
          indexedDB: input.indexedDB,
          IDBKeyRange: input.IDBKeyRange,
        }
      : undefined;

    this.namespace = assertNamespace(input.namespace);
    this.db = new RowStoreDexieDatabase<S>(input.dbName, options);
  }

  async get<C extends CollectionId<S>>(
    collectionId: C,
    id: RowId,
  ): Promise<StoredRow<S, C> | undefined> {
    const row = await this.db.rows.get([this.namespace, collectionId, id]);
    return row ? (toStoredRow<S>(row) as StoredRow<S, C>) : undefined;
  }

  async getAll<C extends CollectionId<S>>(collectionId: C): Promise<Array<StoredRow<S, C>>> {
    const rows = await this.db.rows
      .where("[namespace+collectionId]")
      .equals([this.namespace, collectionId])
      .toArray();
    return rows.map((row) => toStoredRow<S>(row) as StoredRow<S, C>);
  }

  async getAllWithParent<C extends CollectionId<S>>(
    collectionId: C,
    parentId: RowId,
  ): Promise<Array<StoredRow<S, C>>> {
    const rows = await this.db.rows
      .where("[namespace+collectionId+parentId]")
      .equals([this.namespace, collectionId, parentId])
      .toArray();
    return rows.map((row) => toStoredRow<S>(row) as StoredRow<S, C>);
  }

  async applyRows(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<Array<RowApplyOutcome<S>>> {
    return this.db.transaction("rw", this.db.rows, async () => {
      if (rows.length === 0) {
        return [];
      }

      for (const row of rows) {
        if (row.namespace !== this.namespace) {
          throw new Error(
            `Namespace mismatch: adapter namespace is "${this.namespace}" but received "${row.namespace}"`,
          );
        }
      }

      const keys: [string, string, string][] = rows.map((row) => [
        this.namespace,
        row.collectionId,
        row.id,
      ]);
      const existingRecords = await this.db.rows.bulkGet(keys);
      const existingByKey = new Map<string, AnyStoredRow<S>>();

      for (let index = 0; index < rows.length; index += 1) {
        const record = existingRecords[index];
        if (record) {
          existingByKey.set(
            `${record.namespace}::${record.collectionId}::${record.id}`,
            toStoredRow(record),
          );
        }
      }

      const winners = new Map<string, AnyStoredRow<S>>();
      const outcomes: RowApplyOutcome<S>[] = [];

      for (const row of rows) {
        const key = `${this.namespace}::${row.collectionId}::${row.id}`;
        const existing = winners.get(key) ?? existingByKey.get(key);
        const written = !existing || compareHlc(row, existing) === 1;

        if (written) {
          winners.set(key, row);
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

      if (winners.size > 0) {
        await this.db.rows.bulkPut(
          [...winners.values()].map((row) => toIndexedDbRow(this.namespace, row)),
        );
      }

      return outcomes;
    });
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

  async getRawRow(collectionId: string, id: string): Promise<IndexedDbRowRecord<S> | undefined> {
    return this.db.rows.get([this.namespace, collectionId, id]);
  }

  close(): void {
    this.db.close();
  }

  async deleteDatabase(): Promise<void> {
    await this.db.delete();
  }
}

export function createIndexedDbRowStoreAdapter<
  S extends CollectionValueMap = Record<string, unknown>,
>(input: CreateIndexedDbRowStoreAdapterInput): IndexedDbRowStoreAdapter<S> {
  return new IndexedDbRowStoreAdapter<S>(input);
}
