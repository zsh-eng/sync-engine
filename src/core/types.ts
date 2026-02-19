export type CollectionValueMap = object;

export type CollectionId<S extends CollectionValueMap> = Extract<keyof S, string>;
export type Namespace = string;
export type RowId = string;
export type PendingSequence = number;

// Zod compatibility without taking a runtime dependency on zod.
// Works with zod schemas because zod types carry an `_output` type member.
export type SchemaLike<TOutput> = { _output: TOutput };

export type InferCollectionsFromSchemas<Schemas extends Record<string, SchemaLike<unknown>>> = {
  [Key in keyof Schemas]: Schemas[Key]["_output"];
};

export interface HlcFields {
  hlcTimestampMs: number;
  hlcCounter: number;
  hlcDeviceId: string;
}

export interface StoredRow<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> extends HlcFields {
  namespace: Namespace;
  collectionId: C;
  id: RowId;
  parentId: RowId | null;
  data: S[C] | null;
  tombstone: boolean;
  txId?: string;
  schemaVersion?: number;
  committedTimestampMs: number;
}

export type AnyStoredRow<S extends CollectionValueMap> = StoredRow<S, CollectionId<S>>;

export interface RowQuery<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> {
  collectionId: C;
  id?: RowId;
  parentId?: RowId;
  includeTombstones?: boolean;
}

export interface StoragePutOptions {
  parentId?: RowId | null;
  txId?: string;
  schemaVersion?: number;
}

export interface StorageAtomicPutOperation<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> {
  type: "put";
  collectionId: C;
  id: RowId;
  data: S[C];
  parentId?: RowId | null;
  txId?: string;
  schemaVersion?: number;
}

export interface StorageAtomicDeleteOperation<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> {
  type: "delete";
  collectionId: C;
  id: RowId;
}

export type StorageAtomicOperation<S extends CollectionValueMap> =
  | StorageAtomicPutOperation<S>
  | StorageAtomicDeleteOperation<S>;

export interface StorageWriteResult<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> extends HlcFields {
  collectionId: C;
  id: RowId;
  parentId: RowId | null;
  tombstone: boolean;
  committedTimestampMs: number;
  applied: boolean;
}

export interface StorageInvalidationHint<S extends CollectionValueMap> {
  collectionId: CollectionId<S>;
  id?: RowId;
  parentId?: RowId;
}

export interface StorageChangeEvent<S extends CollectionValueMap> {
  source: "local" | "remote";
  invalidationHints: Array<StorageInvalidationHint<S>>;
}

export type StorageListener<S extends CollectionValueMap> = (event: StorageChangeEvent<S>) => void;

export interface PendingPutOperation<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> extends HlcFields {
  sequence: PendingSequence;
  type: "put";
  collectionId: C;
  id: RowId;
  parentId: RowId | null;
  data: S[C];
  txId?: string;
  schemaVersion?: number;
}

export interface PendingDeleteOperation<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> extends HlcFields {
  sequence: PendingSequence;
  type: "delete";
  collectionId: C;
  id: RowId;
  parentId: RowId | null;
  txId?: string;
  schemaVersion?: number;
}

export type PendingOperation<S extends CollectionValueMap> =
  | PendingPutOperation<S>
  | PendingDeleteOperation<S>;

export interface RowApplyOutcome<
  S extends CollectionValueMap,
  C extends CollectionId<S> = CollectionId<S>,
> extends HlcFields {
  written: boolean;
  collectionId: C;
  id: RowId;
  parentId: RowId | null;
  tombstone: boolean;
  committedTimestampMs: number;
}

export interface RowStorageAdapter<S extends CollectionValueMap> {
  query<C extends CollectionId<S>>(query: RowQuery<S, C>): Promise<Array<StoredRow<S, C>>>;
  applyRows(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<Array<RowApplyOutcome<S>>>;
  appendPending(operations: ReadonlyArray<PendingOperation<S>>): Promise<void>;
  getPending(limit: number): Promise<Array<PendingOperation<S>>>;
  removePendingThrough(sequenceInclusive: PendingSequence): Promise<void>;
  putKV(key: string, value: unknown): Promise<void>;
  getKV<Value = unknown>(key: string): Promise<Value | undefined>;
  deleteKV(key: string): Promise<void>;
}

export interface ApplyRemoteResult<S extends CollectionValueMap> {
  appliedCount: number;
  invalidationHints: Array<StorageInvalidationHint<S>>;
}

export interface Storage<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
> {
  get<C extends CollectionId<S>>(collectionId: C, id: RowId): Promise<StoredRow<S, C> | undefined>;
  getAll<C extends CollectionId<S>>(collectionId: C): Promise<Array<StoredRow<S, C>>>;
  getAllWithParent<C extends CollectionId<S>>(
    collectionId: C,
    parentId: RowId,
  ): Promise<Array<StoredRow<S, C>>>;
  put<C extends CollectionId<S>>(
    collectionId: C,
    id: RowId,
    data: S[C],
    options?: StoragePutOptions,
  ): Promise<StorageWriteResult<S, C>>;
  delete<C extends CollectionId<S>>(collectionId: C, id: RowId): Promise<StorageWriteResult<S, C>>;
  deleteAllWithParent<C extends CollectionId<S>>(
    collectionId: C,
    parentId: RowId,
  ): Promise<Array<StorageWriteResult<S, C>>>;
  batchLocal(
    operations: ReadonlyArray<StorageAtomicOperation<S>>,
  ): Promise<Array<StorageWriteResult<S>>>;
  applyRemote(rows: ReadonlyArray<AnyStoredRow<S>>): Promise<ApplyRemoteResult<S>>;
  getPending(limit: number): Promise<Array<PendingOperation<S>>>;
  removePendingThrough(sequenceInclusive: PendingSequence): Promise<void>;
  putKV<Key extends keyof KV & string>(key: Key, value: KV[Key]): Promise<void>;
  getKV<Key extends keyof KV & string>(key: Key): Promise<KV[Key] | undefined>;
  deleteKV<Key extends keyof KV & string>(key: Key): Promise<void>;
  subscribe(listener: StorageListener<S>): () => void;
}

export type StorageFactory<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
  Options = unknown,
> = (namespace: Namespace, adapter: RowStorageAdapter<S>, options?: Options) => Storage<S, KV>;

export type ConnectionState = "offline" | "connected" | "needsAuth" | "paused";

export interface ConnectionDriver {
  subscribe(listener: (state: ConnectionState) => void): () => void;
}

export interface ConnectionManager {
  getState(): ConnectionState;
  subscribe(listener: (state: ConnectionState) => void): () => void;
}

export interface SyncCursor {
  committedTimestampMs: number;
  collectionId: string;
  id: RowId;
}

export interface TransportPullRequest<S extends CollectionValueMap> {
  cursor?: SyncCursor;
  limit: number;
  collectionId?: CollectionId<S>;
  parentId?: RowId;
}

export interface TransportPullResponse<S extends CollectionValueMap> {
  changes: Array<AnyStoredRow<S>>;
  nextCursor?: SyncCursor;
  hasMore: boolean;
}

export interface TransportPushRequest<S extends CollectionValueMap> {
  operations: Array<PendingOperation<S>>;
}

export interface TransportPushResponse {
  acknowledgedThroughSequence?: PendingSequence;
}

export type TransportEvent<S extends CollectionValueMap> =
  | {
      type: "serverChanges";
      changes: Array<AnyStoredRow<S>>;
    }
  | {
      type: "needsAuth";
    };

export interface TransportAdapter<S extends CollectionValueMap> {
  pull(request: TransportPullRequest<S>): Promise<TransportPullResponse<S>>;
  push(request: TransportPushRequest<S>): Promise<TransportPushResponse>;
  onEvent(listener: (event: TransportEvent<S>) => void): () => void;
}

export interface SyncEngine {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export type SyncEngineFactory<
  S extends CollectionValueMap,
  KV extends Record<string, unknown> = Record<string, unknown>,
> = (
  storage: Storage<S, KV>,
  connectionManager: ConnectionManager,
  transportAdapter: TransportAdapter<S>,
) => Promise<SyncEngine>;
