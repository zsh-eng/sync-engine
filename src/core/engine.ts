import type { ClockService } from "./hlc";
import { parseClock } from "./hlc";
import { createSerialQueue } from "./internal/serial-queue";
import type {
  AnyStoredRow,
  ApplyRemoteResult,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  RowId,
  RowStorageAdapter,
  Storage,
  StorageAtomicOperation,
  StorageChangeEvent,
  StorageInvalidationHint,
  StorageListener,
  StoragePutOptions,
  StorageWriteResult,
  StoredRow,
} from "./types";

interface CreateEngineInput<S extends CollectionValueMap, _KV extends Record<string, unknown>> {
  adapter: RowStorageAdapter<S>;
  clock: Pick<ClockService, "nextBatch">;
  namespace: string;
  txIDFactory?: () => string;
}

interface LocalAtomicIntent<S extends CollectionValueMap> {
  kind: "put" | "delete";
  collectionId: CollectionId<S>;
  id: RowId;
  parentId: RowId | null;
  data: unknown | null;
  txId?: string;
  schemaVersion?: number;
}

export type EngineInvalidationHint<S extends CollectionValueMap> = StorageInvalidationHint<S>;
export type EngineEvent<S extends CollectionValueMap> = StorageChangeEvent<S>;
export type EngineListener<S extends CollectionValueMap> = StorageListener<S>;

export interface Engine<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
> extends Storage<S, KV> {}

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
  let nextPendingSequence = 1;
  const queue = createSerialQueue();

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

  async function queryRow<C extends CollectionId<S>>(
    collectionId: C,
    id: RowId,
    includeTombstones: boolean,
  ): Promise<StoredRow<S, C> | undefined> {
    const rows = await input.adapter.query({
      collectionId,
      id,
      includeTombstones,
    });
    return rows[0];
  }

  async function resolveAtomicIntents(
    operations: ReadonlyArray<StorageAtomicOperation<S>>,
  ): Promise<LocalAtomicIntent<S>[]> {
    const intents: LocalAtomicIntent<S>[] = [];

    for (const operation of operations) {
      switch (operation.type) {
        case "put": {
          let parentId = operation.parentId ?? null;

          if (operation.parentId === undefined) {
            const existing = await queryRow(operation.collectionId, operation.id, true);
            parentId = existing?.parentId ?? null;
          }

          intents.push({
            kind: "put",
            collectionId: operation.collectionId,
            id: operation.id,
            parentId,
            data: operation.data,
            txId: operation.txId,
            schemaVersion: operation.schemaVersion,
          });
          break;
        }
        case "delete": {
          const existing = await queryRow(operation.collectionId, operation.id, true);
          intents.push({
            kind: "delete",
            collectionId: operation.collectionId,
            id: operation.id,
            parentId: existing?.parentId ?? null,
            data: null,
          });
          break;
        }
        default: {
          assertNever(operation);
        }
      }
    }

    return intents;
  }

  async function applyLocalIntents(
    intents: ReadonlyArray<LocalAtomicIntent<S>>,
    defaultTxID: string,
  ): Promise<Array<StorageWriteResult<S>>> {
    if (intents.length === 0) {
      return [];
    }

    const clocks = await input.clock.nextBatch(intents.length);
    const rows: AnyStoredRow<S>[] = intents.map((intent, index) => {
      const clock = clocks[index]!;
      const parsed = parseClock(clock);

      return {
        namespace: input.namespace,
        collectionId: intent.collectionId,
        id: intent.id,
        parentId: intent.parentId,
        data:
          intent.kind === "put" ? (structuredClone(intent.data) as AnyStoredRow<S>["data"]) : null,
        tombstone: intent.kind === "delete",
        txId: intent.txId ?? defaultTxID,
        schemaVersion: intent.schemaVersion,
        committedTimestampMs: parsed.wallMs,
        hlcTimestampMs: parsed.wallMs,
        hlcCounter: parsed.counter,
        hlcDeviceId: parsed.nodeId,
      };
    });

    const outcomes = await input.adapter.applyRows(rows);
    const results: StorageWriteResult<S>[] = [];
    const invalidationHints: Array<EngineInvalidationHint<S>> = [];
    const pendingOperations: PendingOperation<S>[] = [];

    for (let index = 0; index < outcomes.length; index += 1) {
      const outcome = outcomes[index]!;
      const row = rows[index]!;
      results.push(buildWriteResult(outcome));

      if (!outcome.written) {
        continue;
      }

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
          schemaVersion: row.schemaVersion,
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
          schemaVersion: row.schemaVersion,
          hlcTimestampMs: row.hlcTimestampMs,
          hlcCounter: row.hlcCounter,
          hlcDeviceId: row.hlcDeviceId,
        });
      }
    }

    if (pendingOperations.length > 0) {
      await input.adapter.appendPending(pendingOperations);
    }

    notify("local", uniqueInvalidationHints(invalidationHints));
    return results;
  }

  return {
    async get<C extends CollectionId<S>>(
      collectionId: C,
      id: RowId,
    ): Promise<StoredRow<S, C> | undefined> {
      return queue.run(async () => {
        const row = await queryRow(collectionId, id, true);
        return asLiveRow(row as AnyStoredRow<S> | undefined) as StoredRow<S, C> | undefined;
      });
    },

    async getAll<C extends CollectionId<S>>(collectionId: C): Promise<Array<StoredRow<S, C>>> {
      return queue.run(async () => {
        const rows = await input.adapter.query({
          collectionId,
          includeTombstones: false,
        });
        return asLiveRows(rows as AnyStoredRow<S>[]) as Array<StoredRow<S, C>>;
      });
    },

    async getAllWithParent<C extends CollectionId<S>>(
      collectionId: C,
      parentId: RowId,
    ): Promise<Array<StoredRow<S, C>>> {
      return queue.run(async () => {
        const rows = await input.adapter.query({
          collectionId,
          parentId,
          includeTombstones: false,
        });
        return asLiveRows(rows as AnyStoredRow<S>[]) as Array<StoredRow<S, C>>;
      });
    },

    async put<C extends CollectionId<S>>(
      collectionId: C,
      id: RowId,
      data: S[C],
      options?: StoragePutOptions,
    ): Promise<StorageWriteResult<S, C>> {
      return queue.run(async () => {
        const intents = await resolveAtomicIntents([
          {
            type: "put",
            collectionId,
            id,
            data,
            parentId: options?.parentId,
            txId: options?.txId,
            schemaVersion: options?.schemaVersion,
          },
        ]);
        const [result] = await applyLocalIntents(intents, txIDFactory());
        return result as StorageWriteResult<S, C>;
      });
    },

    async delete<C extends CollectionId<S>>(
      collectionId: C,
      id: RowId,
    ): Promise<StorageWriteResult<S, C>> {
      return queue.run(async () => {
        const intents = await resolveAtomicIntents([{ type: "delete", collectionId, id }]);
        const [result] = await applyLocalIntents(intents, txIDFactory());
        return result as StorageWriteResult<S, C>;
      });
    },

    async deleteAllWithParent<C extends CollectionId<S>>(
      collectionId: C,
      parentId: RowId,
    ): Promise<Array<StorageWriteResult<S, C>>> {
      return queue.run(async () => {
        const rows = await input.adapter.query({
          collectionId,
          parentId,
          includeTombstones: false,
        });
        const liveRows = rows.filter((row) => !row.tombstone);

        if (liveRows.length === 0) {
          return [];
        }

        const intents: LocalAtomicIntent<S>[] = liveRows.map((row) => ({
          kind: "delete",
          collectionId: row.collectionId,
          id: row.id,
          parentId: row.parentId,
          data: null,
        }));
        const results = await applyLocalIntents(intents, txIDFactory());
        return results as Array<StorageWriteResult<S, C>>;
      });
    },

    async batchLocal(
      operations: ReadonlyArray<StorageAtomicOperation<S>>,
    ): Promise<Array<StorageWriteResult<S>>> {
      if (operations.length === 0) {
        return [];
      }

      return queue.run(async () => {
        const intents = await resolveAtomicIntents(operations);
        return applyLocalIntents(intents, txIDFactory());
      });
    },

    async applyRemote(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<ApplyRemoteResult<S>> {
      if (rows.length === 0) {
        return { appliedCount: 0, invalidationHints: [] };
      }

      return queue.run(async () => {
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

    async getPending(limit: number): Promise<Array<PendingOperation<S>>> {
      return input.adapter.getPending(limit);
    },

    async removePendingThrough(sequenceInclusive: number): Promise<void> {
      await input.adapter.removePendingThrough(sequenceInclusive);
    },

    async putKV<Key extends keyof KV & string>(key: Key, value: KV[Key]): Promise<void> {
      await input.adapter.putKV(key, structuredClone(value));
    },

    async getKV<Key extends keyof KV & string>(key: Key): Promise<KV[Key] | undefined> {
      const value = await input.adapter.getKV(key);
      return value === undefined ? undefined : (structuredClone(value) as KV[Key]);
    },

    async deleteKV<Key extends keyof KV & string>(key: Key): Promise<void> {
      await input.adapter.deleteKV(key);
    },

    subscribe(listener: EngineListener<S>): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
