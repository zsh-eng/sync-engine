import type { RowStoreAdapter, RowStoreAdapterTransaction, StoredRow } from "./types";

function rowKey(collection: string, id: string): string {
  return `${collection}::${id}`;
}

function cloneRow<Value>(row: StoredRow<Value>): StoredRow<Value> {
  return {
    ...row,
    value: row.value === null ? null : structuredClone(row.value),
  };
}

function cloneRowsMap<Value>(
  rows: ReadonlyMap<string, StoredRow<Value>>,
): Map<string, StoredRow<Value>> {
  const next = new Map<string, StoredRow<Value>>();
  for (const [key, row] of rows.entries()) {
    next.set(key, cloneRow(row));
  }
  return next;
}

interface CreateInMemoryRowStoreAdapterInput<Value = unknown> {
  seedRows?: ReadonlyArray<StoredRow<Value>>;
}

export class InMemoryRowStoreAdapter<Value = unknown> implements RowStoreAdapter<Value> {
  private rows = new Map<string, StoredRow<Value>>();

  constructor(input: CreateInMemoryRowStoreAdapterInput<Value> = {}) {
    if (!input.seedRows) {
      return;
    }

    for (const row of input.seedRows) {
      this.rows.set(rowKey(row.collection, row.id), cloneRow(row));
    }
  }

  async txn<Result>(
    runner: (tx: RowStoreAdapterTransaction<Value>) => Promise<Result>,
  ): Promise<Result> {
    const workingRows = cloneRowsMap(this.rows);

    const tx: RowStoreAdapterTransaction<Value> = {
      getAll: async (collection) => {
        const rows: StoredRow<Value>[] = [];
        for (const row of workingRows.values()) {
          if (row.collection === collection) {
            rows.push(cloneRow(row));
          }
        }
        return rows;
      },
      get: async (collection, id) => {
        const row = workingRows.get(rowKey(collection, id));
        return row ? cloneRow(row) : undefined;
      },
      getAllWithParent: async (collection, parentID) => {
        const rows: StoredRow<Value>[] = [];
        for (const row of workingRows.values()) {
          if (row.collection === collection && row.parentID === parentID) {
            rows.push(cloneRow(row));
          }
        }
        return rows;
      },
      bulkPut: async (rows) => {
        for (const row of rows) {
          workingRows.set(rowKey(row.collection, row.id), cloneRow(row));
        }
      },
    };

    const result = await runner(tx);
    this.rows = workingRows;
    return result;
  }

  async dumpAll(): Promise<StoredRow<Value>[]> {
    return [...this.rows.values()].map((row) => cloneRow(row));
  }
}

export function createInMemoryRowStoreAdapter<Value = unknown>(
  input: CreateInMemoryRowStoreAdapterInput<Value> = {},
): InMemoryRowStoreAdapter<Value> {
  return new InMemoryRowStoreAdapter<Value>(input);
}
