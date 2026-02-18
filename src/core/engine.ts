import type { ClockService } from "./hlc";
import { parseClock } from "./hlc";
import type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  RowId,
  RowStorageAdapter,
  Storage,
  StorageOp,
  StorageResult,
  StorageResults,
  StorageWriteResult,
} from "./types";

interface CreateEngineInput<S extends CollectionValueMap, KV extends Record<string, unknown>> {
  adapter: RowStorageAdapter<S>;
  clock: Pick<ClockService, "nextBatch">;
  namespace: string;
  txIDFactory?: () => string;
}

interface WriteIntent<S extends CollectionValueMap> {
  opIndex: number;
  kind: "put" | "delete";
  collectionId: CollectionId<S>;
  id: RowId;
  parentId: RowId | null;
  data: unknown | null;
}

export interface EngineInvalidationHint<S extends CollectionValueMap> {
  collectionId: CollectionId<S>;
  id?: RowId;
  parentId?: RowId;
}

export interface EngineEvent<S extends CollectionValueMap> {
  source: "local" | "remote";
  invalidationHints: Array<EngineInvalidationHint<S>>;
}

export type EngineListener<S extends CollectionValueMap> = (event: EngineEvent<S>) => void;

export interface ApplyRemoteResult<S extends CollectionValueMap> {
  appliedCount: number;
  invalidationHints: Array<EngineInvalidationHint<S>>;
}

export interface Engine<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
> extends Storage<S, KV> {
  applyRemote(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<ApplyRemoteResult<S>>;
  subscribe(listener: EngineListener<S>): () => void;
}

const DEFAULT_TX_ID_PREFIX = "tx";

function defaultTxIDFactory(): string {
  return `${DEFAULT_TX_ID_PREFIX}_${crypto.randomUUID()}`;
}

function asLiveRow<S extends CollectionValueMap>(
  row: AnyStoredRow<S> | undefined,
): AnyStoredRow<S> | undefined {
  if (!row || row.tombstone) {
    return undefined;
  }

  return row;
}

function asLiveRows<S extends CollectionValueMap>(
  rows: ReadonlyArray<AnyStoredRow<S>>,
): AnyStoredRow<S>[] {
  return rows.filter((row) => !row.tombstone);
}

function uniqueInvalidationHints<S extends CollectionValueMap>(
  hints: ReadonlyArray<EngineInvalidationHint<S>>,
): Array<EngineInvalidationHint<S>> {
  const seen = new Set<string>();
  const unique: Array<EngineInvalidationHint<S>> = [];

  for (const hint of hints) {
    const key = `${hint.collectionId}::${hint.id ?? ""}::${hint.parentId ?? ""}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(hint);
  }

  return unique;
}

function assertNever(value: never): never {
  throw new Error(`Unsupported operation: ${String(value)}`);
}

function buildWriteResult<S extends CollectionValueMap>(outcome: {
  written: boolean;
  collectionId: CollectionId<S>;
  id: RowId;
  parentId: RowId | null;
  tombstone: boolean;
  committedTimestampMs: number;
  hlcTimestampMs: number;
  hlcCounter: number;
  hlcDeviceId: string;
}): StorageWriteResult<S> {
  return {
    collectionId: outcome.collectionId,
    id: outcome.id,
    parentId: outcome.parentId,
    tombstone: outcome.tombstone,
    committedTimestampMs: outcome.committedTimestampMs,
    hlcTimestampMs: outcome.hlcTimestampMs,
    hlcCounter: outcome.hlcCounter,
    hlcDeviceId: outcome.hlcDeviceId,
    applied: outcome.written,
  };
}

export function createEngine<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
>(input: CreateEngineInput<S, KV>): Engine<S, KV> {
  const listeners = new Set<EngineListener<S>>();
  const txIDFactory = input.txIDFactory ?? defaultTxIDFactory;
  const kvStore = new Map<string, unknown>();
  let nextPendingSequence = 1;

  // Serialize engine operations to keep planning deterministic.
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

  function notify(source: "local" | "remote", hints: Array<EngineInvalidationHint<S>>): void {
    if (hints.length === 0) {
      return;
    }

    const event: EngineEvent<S> = {
      source,
      invalidationHints: hints,
    };

    for (const listener of listeners) {
      listener(event);
    }
  }

  return {
    async execute<const Ops extends readonly StorageOp<S>[]>(
      operations: Ops,
    ): Promise<StorageResults<S, Ops>> {
      if (operations.length === 0) {
        return [] as StorageResults<S, Ops>;
      }

      return enqueue(async () => {
        const txID = txIDFactory();
        const results: unknown[] = Array.from({ length: operations.length });
        const writeIntents: Array<WriteIntent<S>> = [];
        const writeIntentIndexesByOp = new Map<number, number[]>();

        for (let opIndex = 0; opIndex < operations.length; opIndex += 1) {
          const operation = operations[opIndex]!;

          switch (operation.type) {
            case "get": {
              const row = await input.adapter.get(operation.collectionId, operation.id);
              results[opIndex] = asLiveRow(row) as StorageResult<S, typeof operation>;
              break;
            }
            case "getAll": {
              const rows = await input.adapter.getAll(operation.collectionId);
              results[opIndex] = asLiveRows(rows) as StorageResult<S, typeof operation>;
              break;
            }
            case "getAllWithParent": {
              const rows = await input.adapter.getAllWithParent(
                operation.collectionId,
                operation.parentId,
              );
              results[opIndex] = asLiveRows(rows) as StorageResult<S, typeof operation>;
              break;
            }
            case "put": {
              let parentId = operation.parentId ?? null;

              if (operation.parentId === undefined) {
                const existing = await input.adapter.get(operation.collectionId, operation.id);
                parentId = existing?.parentId ?? null;
              }

              const intentIndex = writeIntents.length;
              writeIntents.push({
                opIndex,
                kind: "put",
                collectionId: operation.collectionId,
                id: operation.id,
                parentId,
                data: operation.data,
              });
              writeIntentIndexesByOp.set(opIndex, [intentIndex]);
              break;
            }
            case "delete": {
              const existing = await input.adapter.get(operation.collectionId, operation.id);
              const intentIndex = writeIntents.length;

              writeIntents.push({
                opIndex,
                kind: "delete",
                collectionId: operation.collectionId,
                id: operation.id,
                parentId: existing?.parentId ?? null,
                data: null,
              });
              writeIntentIndexesByOp.set(opIndex, [intentIndex]);
              break;
            }
            case "deleteAllWithParent": {
              const rows = await input.adapter.getAllWithParent(
                operation.collectionId,
                operation.parentId,
              );
              const liveRows = rows.filter((row) => !row.tombstone);
              const intentIndexes: number[] = [];

              for (const row of liveRows) {
                const intentIndex = writeIntents.length;
                writeIntents.push({
                  opIndex,
                  kind: "delete",
                  collectionId: row.collectionId,
                  id: row.id,
                  parentId: row.parentId,
                  data: null,
                });
                intentIndexes.push(intentIndex);
              }

              writeIntentIndexesByOp.set(opIndex, intentIndexes);
              if (intentIndexes.length === 0) {
                results[opIndex] = [];
              }
              break;
            }
            default: {
              assertNever(operation);
            }
          }
        }

        if (writeIntents.length === 0) {
          return results as StorageResults<S, Ops>;
        }

        const clocks = await input.clock.nextBatch(writeIntents.length);
        const rows: AnyStoredRow<S>[] = writeIntents.map((intent, index) => {
          const clock = clocks[index]!;
          const parsed = parseClock(clock);

          return {
            namespace: input.namespace,
            collectionId: intent.collectionId,
            id: intent.id,
            parentId: intent.parentId,
            data:
              intent.kind === "put"
                ? (structuredClone(intent.data) as AnyStoredRow<S>["data"])
                : null,
            tombstone: intent.kind === "delete",
            txId: txID,
            committedTimestampMs: parsed.wallMs,
            hlcTimestampMs: parsed.wallMs,
            hlcCounter: parsed.counter,
            hlcDeviceId: parsed.nodeId,
          };
        });

        const outcomes = await input.adapter.applyRows(rows);
        const invalidationHints: Array<EngineInvalidationHint<S>> = [];
        const pendingOperations: PendingOperation<S>[] = [];

        for (let opIndex = 0; opIndex < operations.length; opIndex += 1) {
          const operation = operations[opIndex]!;
          const intentIndexes = writeIntentIndexesByOp.get(opIndex);
          if (!intentIndexes) {
            continue;
          }

          const opWriteResults: StorageWriteResult<S>[] = [];

          for (const intentIndex of intentIndexes) {
            const outcome = outcomes[intentIndex]!;
            const row = rows[intentIndex]!;
            const writeResult = buildWriteResult(outcome);
            opWriteResults.push(writeResult);

            if (outcome.written) {
              invalidationHints.push({
                collectionId: outcome.collectionId,
                id: outcome.id,
                ...(outcome.parentId ? { parentId: outcome.parentId } : {}),
              });

              if (row.tombstone) {
                pendingOperations.push({
                  sequence: nextPendingSequence++,
                  type: "delete",
                  collectionId: row.collectionId,
                  id: row.id,
                  parentId: row.parentId,
                  txId: row.txId,
                  hlcTimestampMs: row.hlcTimestampMs,
                  hlcCounter: row.hlcCounter,
                  hlcDeviceId: row.hlcDeviceId,
                });
              } else {
                pendingOperations.push({
                  sequence: nextPendingSequence++,
                  type: "put",
                  collectionId: row.collectionId,
                  id: row.id,
                  parentId: row.parentId,
                  data: structuredClone(row.data) as Exclude<typeof row.data, null>,
                  txId: row.txId,
                  hlcTimestampMs: row.hlcTimestampMs,
                  hlcCounter: row.hlcCounter,
                  hlcDeviceId: row.hlcDeviceId,
                });
              }
            }
          }

          switch (operation.type) {
            case "put":
            case "delete": {
              results[opIndex] = opWriteResults[0];
              break;
            }
            case "deleteAllWithParent": {
              results[opIndex] = opWriteResults;
              break;
            }
            case "get":
            case "getAll":
            case "getAllWithParent": {
              break;
            }
            default: {
              assertNever(operation);
            }
          }
        }

        if (pendingOperations.length > 0) {
          await input.adapter.appendPending(pendingOperations);
        }

        notify("local", uniqueInvalidationHints(invalidationHints));
        return results as StorageResults<S, Ops>;
      });
    },

    async getPending(limit: number): Promise<Array<PendingOperation<S>>> {
      return input.adapter.getPending(limit);
    },

    async removePendingThrough(sequenceInclusive: number): Promise<void> {
      await input.adapter.removePendingThrough(sequenceInclusive);
    },

    async putKV<Key extends keyof KV & string>(key: Key, value: KV[Key]): Promise<void> {
      kvStore.set(key, structuredClone(value));
    },

    async getKV<Key extends keyof KV & string>(key: Key): Promise<KV[Key] | undefined> {
      const value = kvStore.get(key);
      return value === undefined ? undefined : (structuredClone(value) as KV[Key]);
    },

    async deleteKV<Key extends keyof KV & string>(key: Key): Promise<void> {
      kvStore.delete(key);
    },

    async applyRemote(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<ApplyRemoteResult<S>> {
      if (rows.length === 0) {
        return { appliedCount: 0, invalidationHints: [] };
      }

      return enqueue(async () => {
        const outcomes = await input.adapter.applyRows([...rows]);
        const invalidationHints: Array<EngineInvalidationHint<S>> = [];
        let appliedCount = 0;

        for (const outcome of outcomes) {
          if (!outcome.written) {
            continue;
          }

          appliedCount += 1;
          invalidationHints.push({
            collectionId: outcome.collectionId,
            id: outcome.id,
            ...(outcome.parentId ? { parentId: outcome.parentId } : {}),
          });
        }

        const uniqueHints = uniqueInvalidationHints(invalidationHints);
        notify("remote", uniqueHints);

        return {
          appliedCount,
          invalidationHints: uniqueHints,
        };
      });
    },

    subscribe(listener: EngineListener<S>): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
