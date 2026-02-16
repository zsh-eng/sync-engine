import { compareClocks, type ClockService, type HybridLogicalClock } from "../hlc";
import type {
  RowStoreAdapter,
  RowStoreInvalidationHint,
  RowStoreListener,
  RowStoreOperation,
  RowStoreReadResult,
  RowStoreTxnResult,
  RowStoreWriteResult,
  StoredRow,
} from "./types";

interface CreateRowStoreInput<Value = unknown> {
  adapter: RowStoreAdapter<Value>;
  clock: Pick<ClockService, "nextBatch">;
  txIDFactory?: () => string;
}

interface PutIntent<Value = unknown> {
  kind: "put";
  collection: string;
  id: string;
  parentID: string | null;
  value: Value;
}

interface DeleteIntent {
  kind: "delete";
  collection: string;
  id: string;
  parentID: string | null;
}

type WriteIntent<Value = unknown> = PutIntent<Value> | DeleteIntent;

export interface RowStore<Value = unknown> {
  txn(operations: ReadonlyArray<RowStoreOperation<Value>>): Promise<RowStoreTxnResult<Value>>;
  subscribe(listener: RowStoreListener<Value>): () => void;
}

const DEFAULT_TX_ID_PREFIX = "tx";

function defaultTxIDFactory(): string {
  return `${DEFAULT_TX_ID_PREFIX}_${crypto.randomUUID()}`;
}

function asLiveRow<Value>(row: StoredRow<Value> | undefined): StoredRow<Value> | undefined {
  if (!row || row.tombstone) {
    return undefined;
  }

  return row;
}

function asLiveRows<Value>(rows: ReadonlyArray<StoredRow<Value>>): StoredRow<Value>[] {
  return rows.filter((row) => !row.tombstone);
}

function uniqueInvalidationHints(
  hints: ReadonlyArray<RowStoreInvalidationHint>,
): RowStoreInvalidationHint[] {
  const seen = new Set<string>();
  const unique: RowStoreInvalidationHint[] = [];

  for (const hint of hints) {
    const key = `${hint.collection}::${hint.id ?? ""}::${hint.parentID ?? ""}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(hint);
  }

  return unique;
}

function rowKey(collection: string, id: string): string {
  return `${collection}::${id}`;
}

function assertNever(value: never): never {
  throw new Error(`Unsupported operation kind: ${String(value)}`);
}

export function createRowStore<Value = unknown>(
  input: CreateRowStoreInput<Value>,
): RowStore<Value> {
  const listeners = new Set<RowStoreListener<Value>>();
  const txIDFactory = input.txIDFactory ?? defaultTxIDFactory;

  // Serialize transactions to keep operation planning deterministic.
  let queue: Promise<void> = Promise.resolve();

  async function enqueue<Result>(operation: () => Promise<Result>): Promise<Result> {
    const previous = queue;
    let release: () => void = () => undefined;
    queue = new Promise<void>((resolve) => {
      release = resolve;
    });

    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  function notify(result: RowStoreTxnResult<Value>): void {
    if (result.invalidationHints.length === 0) {
      return;
    }

    for (const listener of listeners) {
      listener(result);
    }
  }

  return {
    async txn(
      operations: ReadonlyArray<RowStoreOperation<Value>>,
    ): Promise<RowStoreTxnResult<Value>> {
      if (operations.length === 0) {
        return {
          txID: txIDFactory(),
          readResults: [],
          writes: [],
          invalidationHints: [],
        };
      }

      return enqueue(async () => {
        const txID = txIDFactory();
        const planned = await input.adapter.txn(async (tx) => {
          const readResults: RowStoreReadResult<Value>[] = [];
          const writeIntents: WriteIntent<Value>[] = [];

          for (let opIndex = 0; opIndex < operations.length; opIndex += 1) {
            const operation = operations[opIndex]!;

            switch (operation.kind) {
              case "get_all": {
                const rows = await tx.getAll(operation.collection);
                readResults.push({
                  opIndex,
                  kind: "get_all",
                  rows: asLiveRows(rows),
                });
                break;
              }
              case "get": {
                const row = await tx.get(operation.collection, operation.id);
                readResults.push({
                  opIndex,
                  kind: "get",
                  row: asLiveRow(row),
                });
                break;
              }
              case "get_all_with_parent": {
                const rows = await tx.getAllWithParent(operation.collection, operation.parentID);
                readResults.push({
                  opIndex,
                  kind: "get_all_with_parent",
                  rows: asLiveRows(rows),
                });
                break;
              }
              case "put": {
                let parentID = operation.parentID ?? null;
                if (operation.parentID === undefined) {
                  const existing = await tx.get(operation.collection, operation.id);
                  parentID = existing?.parentID ?? null;
                }

                writeIntents.push({
                  kind: "put",
                  collection: operation.collection,
                  id: operation.id,
                  parentID,
                  value: operation.value,
                });
                break;
              }
              case "delete": {
                const existing = await tx.get(operation.collection, operation.id);
                writeIntents.push({
                  kind: "delete",
                  collection: operation.collection,
                  id: operation.id,
                  parentID: existing?.parentID ?? null,
                });
                break;
              }
              case "delete_all_with_parent": {
                const rows = await tx.getAllWithParent(operation.collection, operation.parentID);
                for (const row of rows) {
                  if (row.tombstone) {
                    continue;
                  }

                  writeIntents.push({
                    kind: "delete",
                    collection: operation.collection,
                    id: row.id,
                    parentID: row.parentID,
                  });
                }
                break;
              }
              default: {
                assertNever(operation);
              }
            }
          }

          return {
            readResults,
            writeIntents,
          };
        });

        if (planned.writeIntents.length === 0) {
          return {
            txID,
            readResults: planned.readResults,
            writes: [],
            invalidationHints: [],
          };
        }

        const clocks: HybridLogicalClock[] = await input.clock.nextBatch(
          planned.writeIntents.length,
        );

        const applied = await input.adapter.txn(async (tx) => {
          const stagedRows = new Map<string, StoredRow<Value>>();
          const writes: RowStoreWriteResult[] = [];
          const invalidationHints: RowStoreInvalidationHint[] = [];

          for (let index = 0; index < planned.writeIntents.length; index += 1) {
            const intent = planned.writeIntents[index]!;
            const hlc = clocks[index]!;
            if (!hlc) {
              throw new Error("Missing HLC for write intent");
            }

            let row: StoredRow<Value>;
            let write: RowStoreWriteResult;
            let invalidationHint: RowStoreInvalidationHint;

            switch (intent.kind) {
              case "put": {
                row = {
                  collection: intent.collection,
                  id: intent.id,
                  parentID: intent.parentID,
                  value: intent.value,
                  hlc,
                  txID,
                  tombstone: false,
                };
                write = {
                  collection: intent.collection,
                  id: intent.id,
                  parentID: intent.parentID,
                  hlc,
                  tombstone: false,
                };
                invalidationHint = {
                  collection: intent.collection,
                  id: intent.id,
                  ...(intent.parentID ? { parentID: intent.parentID } : {}),
                };
                break;
              }
              case "delete": {
                row = {
                  collection: intent.collection,
                  id: intent.id,
                  parentID: intent.parentID,
                  value: null,
                  hlc,
                  txID,
                  tombstone: true,
                };
                write = {
                  collection: intent.collection,
                  id: intent.id,
                  parentID: intent.parentID,
                  hlc,
                  tombstone: true,
                };
                invalidationHint = {
                  collection: intent.collection,
                  id: intent.id,
                  ...(intent.parentID ? { parentID: intent.parentID } : {}),
                };
                break;
              }
              default: {
                assertNever(intent);
              }
            }

            const key = rowKey(row.collection, row.id);
            const existing = stagedRows.get(key) ?? (await tx.get(row.collection, row.id));
            if (existing && compareClocks(row.hlc, existing.hlc) !== 1) {
              continue;
            }

            stagedRows.set(key, row);
            writes.push(write);
            invalidationHints.push(invalidationHint);
          }

          if (stagedRows.size > 0) {
            await tx.bulkPut([...stagedRows.values()]);
          }

          return {
            writes,
            invalidationHints,
          };
        });

        if (applied.writes.length === 0) {
          return {
            txID,
            readResults: planned.readResults,
            writes: [],
            invalidationHints: [],
          };
        }

        const result = {
          txID,
          readResults: planned.readResults,
          writes: applied.writes,
          invalidationHints: uniqueInvalidationHints(applied.invalidationHints),
        };

        notify(result);
        return result;
      });
    },

    subscribe(listener: RowStoreListener<Value>): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
