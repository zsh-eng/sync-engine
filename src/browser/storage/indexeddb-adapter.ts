import Dexie, { type Table } from "dexie";

import { compareClocks, formatClock, parseClock } from "../../core/hlc";
import type {
  RowStoreAdapter,
  RowStoreAdapterTransaction,
  StoredRow,
  WriteOutcome,
} from "../../core/storage/types";

export interface IndexedDbRowRecord<Value = unknown> {
  namespace: string;
  collection: string;
  id: string;
  parentID: string | null;
  value: Value | null;
  hlcWallMs: number;
  hlcCounter: number;
  hlcNodeId: string;
  txID: string;
  tombstone: 0 | 1;
}

class RowStoreDexieDatabase<Value = unknown> extends Dexie {
  readonly rows!: Table<IndexedDbRowRecord<Value>, [string, string, string]>;

  constructor(
    name: string,
    options?: {
      indexedDB?: IDBFactory;
      IDBKeyRange?: typeof IDBKeyRange;
    },
  ) {
    super(name, options);

    this.version(1).stores({
      rows: "&[namespace+collection+id], [namespace+collection], [namespace+collection+parentID], [namespace+collection+tombstone], [namespace+collection+parentID+tombstone], [namespace+hlcWallMs+hlcCounter], [namespace+hlcWallMs+hlcCounter+hlcNodeId], [namespace+collection+hlcWallMs+hlcCounter+hlcNodeId]",
    });
  }
}

function assertNamespace(value: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("Invalid namespace: expected a non-empty string");
  }

  return value;
}

function toStoredRow<Value>(row: IndexedDbRowRecord<Value>): StoredRow<Value> {
  return {
    collection: row.collection,
    id: row.id,
    parentID: row.parentID,
    value: row.value,
    hlc: formatClock({
      wallMs: row.hlcWallMs,
      counter: row.hlcCounter,
      nodeId: row.hlcNodeId,
    }) as StoredRow<Value>["hlc"],
    txID: row.txID,
    tombstone: row.tombstone === 1,
  };
}

function toIndexedDbRow<Value>(
  namespace: string,
  row: StoredRow<Value>,
): IndexedDbRowRecord<Value> {
  const parsedClock = parseClock(row.hlc);
  return {
    namespace,
    collection: row.collection,
    id: row.id,
    parentID: row.parentID,
    value: row.value,
    hlcWallMs: parsedClock.wallMs,
    hlcCounter: parsedClock.counter,
    hlcNodeId: parsedClock.nodeId,
    txID: row.txID,
    tombstone: row.tombstone ? 1 : 0,
  };
}

export interface CreateIndexedDbRowStoreAdapterInput {
  dbName: string;
  namespace: string;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
}

export class IndexedDbRowStoreAdapter<Value = unknown> implements RowStoreAdapter<Value> {
  private readonly db: RowStoreDexieDatabase<Value>;
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
    this.db = new RowStoreDexieDatabase<Value>(input.dbName, options);
  }

  async txn<Result>(
    runner: (tx: RowStoreAdapterTransaction<Value>) => Promise<Result>,
  ): Promise<Result> {
    return this.db.transaction("rw", this.db.rows, async () => {
      return runner({
        getAll: async (collection) => {
          const rows = await this.db.rows
            .where("[namespace+collection]")
            .equals([this.namespace, collection])
            .toArray();
          return rows.map((row) => toStoredRow(row));
        },
        get: async (collection, id) => {
          const row = await this.db.rows.get([this.namespace, collection, id]);
          return row ? toStoredRow(row) : undefined;
        },
        getAllWithParent: async (collection, parentID) => {
          const rows = await this.db.rows
            .where("[namespace+collection+parentID]")
            .equals([this.namespace, collection, parentID])
            .toArray();
          return rows.map((row) => toStoredRow(row));
        },
        applyRows: async (rows) => {
          if (rows.length === 0) {
            return [];
          }

          // Single round-trip to fetch all existing rows.
          const keys: [string, string, string][] = rows.map((r) => [
            this.namespace,
            r.collection,
            r.id,
          ]);
          const existingRecords = await this.db.rows.bulkGet(keys);
          const existingByKey = new Map<string, StoredRow<Value>>();
          for (let i = 0; i < rows.length; i++) {
            const record = existingRecords[i];
            if (record) {
              existingByKey.set(
                `${record.namespace}::${record.collection}::${record.id}`,
                toStoredRow(record),
              );
            }
          }

          const winners = new Map<string, StoredRow<Value>>();
          const outcomes: WriteOutcome[] = [];

          for (const row of rows) {
            const key = `${this.namespace}::${row.collection}::${row.id}`;
            const existing = winners.get(key) ?? existingByKey.get(key);
            const written = !existing || compareClocks(row.hlc, existing.hlc) === 1;
            if (written) {
              winners.set(key, row);
            }
            outcomes.push({
              written,
              collection: row.collection,
              id: row.id,
              parentID: row.parentID,
              hlc: row.hlc,
              tombstone: row.tombstone,
            });
          }

          if (winners.size > 0) {
            await this.db.rows.bulkPut(
              [...winners.values()].map((r) => toIndexedDbRow(this.namespace, r)),
            );
          }

          return outcomes;
        },
      });
    });
  }

  async getRawRow(collection: string, id: string): Promise<IndexedDbRowRecord<Value> | undefined> {
    return this.db.rows.get([this.namespace, collection, id]);
  }

  close(): void {
    this.db.close();
  }

  async deleteDatabase(): Promise<void> {
    await this.db.delete();
  }
}

export function createIndexedDbRowStoreAdapter<Value = unknown>(
  input: CreateIndexedDbRowStoreAdapterInput,
): IndexedDbRowStoreAdapter<Value> {
  return new IndexedDbRowStoreAdapter<Value>(input);
}
