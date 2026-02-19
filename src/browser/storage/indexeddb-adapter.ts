import Dexie, { type Table } from "dexie";

import { compareHlc } from "../../core/storage/shared";
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
} from "../../core/types";

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

export interface IndexedDbKVRecord {
  namespace: string;
  key: string;
  value: unknown;
}

export interface IndexedDbPendingOperationRecord<
  S extends CollectionValueMap = Record<string, unknown>,
> {
  sequence?: number;
  namespace: string;
  operationType: "put" | "delete";
  collectionId: CollectionId<S>;
  id: string;
  parentId: string | null;
  data: unknown | null;
  txId: string | null;
  schemaVersion: number | null;
  hlcTimestampMs: number;
  hlcCounter: number;
  hlcDeviceId: string;
}

class RowStoreDexieDatabase<S extends CollectionValueMap = Record<string, unknown>> extends Dexie {
  readonly rows!: Table<IndexedDbRowRecord<S>, [string, string, string]>;
  readonly kv!: Table<IndexedDbKVRecord, [string, string]>;
  readonly pending!: Table<IndexedDbPendingOperationRecord<S>, number>;

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
      kv: "&[namespace+key]",
    });

    this.version(2).stores({
      rows: "&[namespace+collectionId+id], [namespace+collectionId], [namespace+collectionId+parentId], [namespace+collectionId+tombstone], [namespace+committedTimestampMs+collectionId+id], [namespace+hlcTimestampMs+hlcCounter+hlcDeviceId]",
      kv: "&[namespace+key]",
      pending: "++sequence, [namespace+sequence]",
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

function toPendingRecord<S extends CollectionValueMap>(
  namespace: string,
  operation: PendingOperation<S>,
): IndexedDbPendingOperationRecord<S> {
  if (operation.type === "delete") {
    return {
      namespace,
      operationType: "delete",
      collectionId: operation.collectionId,
      id: operation.id,
      parentId: operation.parentId,
      data: null,
      txId: operation.txId ?? null,
      schemaVersion: operation.schemaVersion ?? null,
      hlcTimestampMs: operation.hlcTimestampMs,
      hlcCounter: operation.hlcCounter,
      hlcDeviceId: operation.hlcDeviceId,
    };
  }

  return {
    namespace,
    operationType: "put",
    collectionId: operation.collectionId,
    id: operation.id,
    parentId: operation.parentId,
    data: structuredClone(operation.data),
    txId: operation.txId ?? null,
    schemaVersion: operation.schemaVersion ?? null,
    hlcTimestampMs: operation.hlcTimestampMs,
    hlcCounter: operation.hlcCounter,
    hlcDeviceId: operation.hlcDeviceId,
  };
}

function toPendingOperation<S extends CollectionValueMap>(
  record: IndexedDbPendingOperationRecord<S>,
): PendingOperation<S> {
  if (record.sequence === undefined) {
    throw new Error("Invalid pending operation: missing sequence");
  }

  if (record.operationType === "delete") {
    return {
      sequence: record.sequence,
      type: "delete",
      collectionId: record.collectionId,
      id: record.id,
      parentId: record.parentId,
      txId: record.txId ?? undefined,
      schemaVersion: record.schemaVersion ?? undefined,
      hlcTimestampMs: record.hlcTimestampMs,
      hlcCounter: record.hlcCounter,
      hlcDeviceId: record.hlcDeviceId,
    };
  }

  if (record.data === null) {
    throw new Error("Invalid pending operation: put operation missing data");
  }

  return {
    sequence: record.sequence,
    type: "put",
    collectionId: record.collectionId,
    id: record.id,
    parentId: record.parentId,
    data: structuredClone(record.data) as S[CollectionId<S>],
    txId: record.txId ?? undefined,
    schemaVersion: record.schemaVersion ?? undefined,
    hlcTimestampMs: record.hlcTimestampMs,
    hlcCounter: record.hlcCounter,
    hlcDeviceId: record.hlcDeviceId,
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

  async query<C extends CollectionId<S>>(query: RowQuery<S, C>): Promise<Array<StoredRow<S, C>>> {
    const includeTombstones = query.includeTombstones === true;
    let rows: IndexedDbRowRecord<S>[];

    if (query.id !== undefined) {
      const row = await this.db.rows.get([this.namespace, query.collectionId, query.id]);
      rows = row ? [row] : [];
    } else if (query.parentId !== undefined) {
      rows = await this.db.rows
        .where("[namespace+collectionId+parentId]")
        .equals([this.namespace, query.collectionId, query.parentId])
        .toArray();
    } else {
      rows = await this.db.rows
        .where("[namespace+collectionId]")
        .equals([this.namespace, query.collectionId])
        .toArray();
    }

    if (query.parentId !== undefined) {
      rows = rows.filter((row) => row.parentId === query.parentId);
    }

    if (!includeTombstones) {
      rows = rows.filter((row) => row.tombstone === 0);
    }

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
    if (operations.length === 0) {
      return;
    }

    await this.db.transaction("rw", this.db.pending, async () => {
      await this.db.pending.bulkAdd(
        operations.map((operation) => toPendingRecord(this.namespace, operation)),
      );
    });
  }

  async getPending(limit: number): Promise<Array<PendingOperation<S>>> {
    const records = await this.db.pending
      .where("[namespace+sequence]")
      .between([this.namespace, Dexie.minKey], [this.namespace, Dexie.maxKey], true, true)
      .limit(Math.max(0, limit))
      .toArray();
    return records.map((record) => toPendingOperation(record));
  }

  async removePendingThrough(sequenceInclusive: PendingSequence): Promise<void> {
    await this.db.pending
      .where("[namespace+sequence]")
      .between([this.namespace, Dexie.minKey], [this.namespace, sequenceInclusive], true, true)
      .delete();
  }

  async putKV(key: string, value: unknown): Promise<void> {
    await this.db.kv.put({
      namespace: this.namespace,
      key,
      value: structuredClone(value),
    });
  }

  async getKV<Value = unknown>(key: string): Promise<Value | undefined> {
    const record = await this.db.kv.get([this.namespace, key]);
    return record ? (structuredClone(record.value) as Value) : undefined;
  }

  async deleteKV(key: string): Promise<void> {
    await this.db.kv.delete([this.namespace, key]);
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
