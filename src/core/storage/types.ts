import type { HybridLogicalClock } from "../hlc";

export interface StoredRow<Value = unknown> {
  collection: string;
  id: string;
  parentID: string | null;
  value: Value | null;
  hlc: HybridLogicalClock;
  txID: string;
  tombstone: boolean;
}

export type RowStoreOperation<Value = unknown> =
  | {
      kind: "get_all";
      collection: string;
    }
  | {
      kind: "get";
      collection: string;
      id: string;
    }
  | {
      kind: "get_all_with_parent";
      collection: string;
      parentID: string;
    }
  | {
      kind: "put";
      collection: string;
      id: string;
      value: Value;
      parentID?: string | null;
    }
  | {
      kind: "delete";
      collection: string;
      id: string;
    }
  | {
      kind: "delete_all_with_parent";
      collection: string;
      parentID: string;
    };

export type RowStoreReadResult<Value = unknown> =
  | {
      opIndex: number;
      kind: "get_all";
      rows: StoredRow<Value>[];
    }
  | {
      opIndex: number;
      kind: "get";
      row?: StoredRow<Value>;
    }
  | {
      opIndex: number;
      kind: "get_all_with_parent";
      rows: StoredRow<Value>[];
    };

export interface RowStoreWriteResult {
  collection: string;
  id: string;
  parentID: string | null;
  hlc: HybridLogicalClock;
  tombstone: boolean;
}

export interface RowStoreInvalidationHint {
  collection: string;
  id?: string;
  parentID?: string;
}

export interface RowStoreTxnResult<Value = unknown> {
  txID: string;
  readResults: RowStoreReadResult<Value>[];
  writes: RowStoreWriteResult[];
  invalidationHints: RowStoreInvalidationHint[];
}

export interface WriteOutcome {
  written: boolean;
  collection: string;
  id: string;
  parentID: string | null;
  hlc: HybridLogicalClock;
  tombstone: boolean;
}

export interface RowStoreAdapterTransaction<Value = unknown> {
  getAll(collection: string): Promise<StoredRow<Value>[]>;
  get(collection: string, id: string): Promise<StoredRow<Value> | undefined>;
  getAllWithParent(collection: string, parentID: string): Promise<StoredRow<Value>[]>;
  applyRows(rows: StoredRow<Value>[]): Promise<WriteOutcome[]>;
}

export interface RowStoreAdapter<Value = unknown> {
  txn<Result>(runner: (tx: RowStoreAdapterTransaction<Value>) => Promise<Result>): Promise<Result>;
}

export type RowStoreListener<Value = unknown> = (result: RowStoreTxnResult<Value>) => void;
