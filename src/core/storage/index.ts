export type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingOperation,
  PendingSequence,
  RowApplyOutcome,
  RowId,
  RowStorageAdapter,
  StoredRow,
  Storage,
  StorageFactory,
  StorageOp,
  StorageResult,
  StorageResults,
  StorageWriteResult,
} from "../types";

export {
  createInMemoryRowStorageAdapter,
  InMemoryRowStorageAdapter,
  type CreateInMemoryRowStorageAdapterInput,
} from "./in-memory-adapter";
