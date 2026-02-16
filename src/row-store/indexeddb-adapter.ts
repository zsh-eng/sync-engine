import Dexie, { type Table } from "dexie";

import { formatClock, parseClock } from "../hlc";
import type { RowStoreAdapter, RowStoreAdapterTransaction, StoredRow } from "./types";

export interface IndexedDbRowRecord<Value = unknown> {
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
  readonly rows!: Table<IndexedDbRowRecord<Value>, [string, string]>;

  constructor(
    name: string,
    options?: {
      indexedDB?: IDBFactory;
      IDBKeyRange?: typeof IDBKeyRange;
    },
  ) {
    super(name, options);

    this.version(1).stores({
      rows: "&[collection+id], collection, [collection+parentID], [collection+tombstone], [collection+parentID+tombstone], [hlcWallMs+hlcCounter], [hlcWallMs+hlcCounter+hlcNodeId], [collection+hlcWallMs+hlcCounter+hlcNodeId]",
    });
  }
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

function toIndexedDbRow<Value>(row: StoredRow<Value>): IndexedDbRowRecord<Value> {
  const parsedClock = parseClock(row.hlc);
  return {
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
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
}

export class IndexedDbRowStoreAdapter<Value = unknown> implements RowStoreAdapter<Value> {
  private readonly db: RowStoreDexieDatabase<Value>;

  constructor(input: CreateIndexedDbRowStoreAdapterInput) {
    const hasCustomIndexedDb = input.indexedDB !== undefined || input.IDBKeyRange !== undefined;
    const options = hasCustomIndexedDb
      ? {
          indexedDB: input.indexedDB,
          IDBKeyRange: input.IDBKeyRange,
        }
      : undefined;

    this.db = new RowStoreDexieDatabase<Value>(input.dbName, options);
  }

  async txn<Result>(
    runner: (tx: RowStoreAdapterTransaction<Value>) => Promise<Result>,
  ): Promise<Result> {
    return this.db.transaction("rw", this.db.rows, async () => {
      return runner({
        getAll: async (collection) => {
          const rows = await this.db.rows.where("collection").equals(collection).toArray();
          return rows.map((row) => toStoredRow(row));
        },
        get: async (collection, id) => {
          const row = await this.db.rows.get([collection, id]);
          return row ? toStoredRow(row) : undefined;
        },
        getAllWithParent: async (collection, parentID) => {
          const rows = await this.db.rows
            .where("[collection+parentID]")
            .equals([collection, parentID])
            .toArray();
          return rows.map((row) => toStoredRow(row));
        },
        bulkPut: async (rows) => {
          if (rows.length === 0) {
            return;
          }

          await this.db.rows.bulkPut(rows.map((row) => toIndexedDbRow(row)));
        },
      });
    });
  }

  async getRawRow(collection: string, id: string): Promise<IndexedDbRowRecord<Value> | undefined> {
    return this.db.rows.get([collection, id]);
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
