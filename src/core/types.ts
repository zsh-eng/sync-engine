type RowOperation = null;
type RowOperationResult = null;
type RowId = null;

// Executes against specific rows
type RowStorageAdapter = {
  // Apply just applies the storage operations with conflict resolution behaviour
  execute: (operations: Array<RowOperation>) => Promise<Array<RowOperationResult>>;
  // applyPending applies + appends to the pending operations (for syncing purposes)
  executeLocal: (operations: Array<RowOperation>) => Promise<Array<RowOperationResult>>;
  removePending: (rowIds: Array<RowId>) => Promise<void>;
}

type StorageOpGet = {
  type: 'get';
  collectionId: string;
  id: string;
}

type StorageOpGetAll = {
  type: 'getAll';
  collectionId: string;
}

type StorageOpGetAllWithParent = {
  type: 'getAllWithParent';
  collectionId: string;
  parentId: string;
}

type StorageOpPut = {
  type: 'put';
  collectionId: string;
  id: string;
  data: any;
}

type StorageOpDelete = {
  type: 'delete';
  collectionId: string;
  id: string;
}

type StorageOpDeleteAllWithParent = {
  type: 'deleteAllWithParent';
  collectionId: string;
  parentId: string;
}

type StorageOp = StorageOpGet | StorageOpGetAll | StorageOpGetAllWithParent | StorageOpPut | StorageOpDelete | StorageOpDeleteAllWithParent;

type StorageFactory = (namespace: string, adapter: RowStorageAdapter, options?: any) => Storage;

// Storage should be some kind of generic type that would give us the right value based on the the operation
// which will also be generic
// The sync engine sends the specified operations to the storage layer
// The storage layer will transform the storage operations into row operations that the adapter can execute
// Why do we need to do this? Because the storage layer need to enrich the operations with HLC etc.
// Storage also maintains a basic key value store that can be used
// For instance, the sync engine uses it to maintain the sync cursors
type Storage = {
  execute: (operations: Array<StorageOp>) => Promise<Array<any>>;

  write: (key: string, value: any) => Promise<void>;
  read: (key: string) => Promise<any>;
}


// We create the connection manager with a specific driver (e.g. browser, or react-native
// that has the bindings to drive the connection manager's FSM)
// The SyncEngine can subscribe to changes in the connection manager to drive its own
// internal sync loop
type ConnectionManager = null;
type ConnectionDriver = null;

type TransportPushRequest = null;
type TransportPushResponse = null;
type TransportPullCursor = null;
// The transport pull responses will most likely be StorageOps that we can execute? Or maybe even row ops for simplicity
type TransportPullResponse = {
  ops: Array<StorageOp>;
  nextCursor: TransportPullCursor;
  hasMore: boolean;
};

type TransportAdapter = {
  pull: (request: TransportPullCursor) => Promise<Array<TransportPullResponse>>;
  push: (requests: Array<TransportPushRequest>) => Promise<Array<TransportPushResponse>>;
  on: (events: Array<TransportPushResponse>) => void;
}

type SyncEngineFactory = (storage: Storage, connectionManager: ConnectionManager, transportAdapter: TransportAdapter) => Promise<SyncEngine>;
// Sync engine will subscribe to connection manager, to manage its periodic polling
// It will also maintain the sync cursors to know what to pull next
type SyncEngine = {
  start: () => Promise<void>;
  stop: () => Promise<void>;
}
