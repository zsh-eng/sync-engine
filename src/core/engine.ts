import type { ClockService, HybridLogicalClock } from "./hlc";
import type {
  RowStoreAdapter,
  RowStoreInvalidationHint,
  RowStoreListener,
  RowStoreOperation,
  RowStoreReadResult,
  RowStoreTxnResult,
  RowStoreWriteResult,
  StoredRow,
  WriteOutcome,
} from "./storage/types";

interface CreateEngineInput<Value = unknown> {
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

export interface ApplyRemoteResult {
  appliedCount: number;
  invalidationHints: RowStoreInvalidationHint[];
}

export interface Engine<Value = unknown> {
  txn(operations: ReadonlyArray<RowStoreOperation<Value>>): Promise<RowStoreTxnResult<Value>>;
  applyRemote(rows: ReadonlyArray<StoredRow<Value>>): Promise<ApplyRemoteResult>;
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

function assertNever(value: never): never {
  throw new Error(`Unsupported operation kind: ${String(value)}`);
}

function outcomesToWritesAndHints(outcomes: ReadonlyArray<WriteOutcome>): {
  writes: RowStoreWriteResult[];
  invalidationHints: RowStoreInvalidationHint[];
} {
  const writes: RowStoreWriteResult[] = [];
  const hints: RowStoreInvalidationHint[] = [];

  for (const outcome of outcomes) {
    if (!outcome.written) {
      continue;
    }

    writes.push({
      collection: outcome.collection,
      id: outcome.id,
      parentID: outcome.parentID,
      hlc: outcome.hlc,
      tombstone: outcome.tombstone,
    });
    hints.push({
      collection: outcome.collection,
      id: outcome.id,
      ...(outcome.parentID ? { parentID: outcome.parentID } : {}),
    });
  }

  return { writes, invalidationHints: uniqueInvalidationHints(hints) };
}

function intentToRow<Value>(
  intent: WriteIntent<Value>,
  hlc: HybridLogicalClock,
  txID: string,
): StoredRow<Value> {
  switch (intent.kind) {
    case "put":
      return {
        collection: intent.collection,
        id: intent.id,
        parentID: intent.parentID,
        value: intent.value,
        hlc,
        txID,
        tombstone: false,
      };
    case "delete":
      return {
        collection: intent.collection,
        id: intent.id,
        parentID: intent.parentID,
        value: null,
        hlc,
        txID,
        tombstone: true,
      };
    default:
      return assertNever(intent);
  }
}

export function createEngine<Value = unknown>(input: CreateEngineInput<Value>): Engine<Value> {
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

  function notifyTxn(result: RowStoreTxnResult<Value>): void {
    if (result.invalidationHints.length === 0) {
      return;
    }

    for (const listener of listeners) {
      listener(result);
    }
  }

  function notifyRemote(result: ApplyRemoteResult): void {
    if (result.invalidationHints.length === 0) {
      return;
    }

    const asTxnResult: RowStoreTxnResult<Value> = {
      txID: "",
      readResults: [],
      writes: [],
      invalidationHints: result.invalidationHints,
    };

    for (const listener of listeners) {
      listener(asTxnResult);
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

        // Phase 1: Plan — dispatch reads and collect write intents.
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

          return { readResults, writeIntents };
        });

        if (planned.writeIntents.length === 0) {
          return {
            txID,
            readResults: planned.readResults,
            writes: [],
            invalidationHints: [],
          };
        }

        // Phase 2: Allocate HLCs outside any adapter transaction.
        const clocks = await input.clock.nextBatch(planned.writeIntents.length);

        // Phase 3: Build StoredRows and apply with conflict resolution.
        const rows: StoredRow<Value>[] = planned.writeIntents.map((intent, i) =>
          intentToRow(intent, clocks[i]!, txID),
        );

        const outcomes = await input.adapter.txn(async (tx) => tx.applyRows(rows));
        const { writes, invalidationHints } = outcomesToWritesAndHints(outcomes);

        if (writes.length === 0) {
          return {
            txID,
            readResults: planned.readResults,
            writes: [],
            invalidationHints: [],
          };
        }

        const result: RowStoreTxnResult<Value> = {
          txID,
          readResults: planned.readResults,
          writes,
          invalidationHints,
        };

        notifyTxn(result);
        return result;
      });
    },

    async applyRemote(rows: ReadonlyArray<StoredRow<Value>>): Promise<ApplyRemoteResult> {
      if (rows.length === 0) {
        return { appliedCount: 0, invalidationHints: [] };
      }

      return enqueue(async () => {
        const outcomes = await input.adapter.txn(async (tx) => tx.applyRows([...rows]));
        const { writes, invalidationHints } = outcomesToWritesAndHints(outcomes);

        const result: ApplyRemoteResult = {
          appliedCount: writes.length,
          invalidationHints,
        };

        notifyRemote(result);
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
