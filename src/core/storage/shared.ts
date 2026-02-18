import type {
  AnyStoredRow,
  CollectionValueMap,
  PendingOperation,
  PendingSequence,
  RowId,
} from "../types";

export function rowKey(collectionId: string, id: RowId): string {
  return `${collectionId}::${id}`;
}

export function compareHlc(
  a: Pick<AnyStoredRow<CollectionValueMap>, "hlcTimestampMs" | "hlcCounter" | "hlcDeviceId">,
  b: Pick<AnyStoredRow<CollectionValueMap>, "hlcTimestampMs" | "hlcCounter" | "hlcDeviceId">,
): -1 | 0 | 1 {
  if (a.hlcTimestampMs !== b.hlcTimestampMs) {
    return a.hlcTimestampMs < b.hlcTimestampMs ? -1 : 1;
  }

  if (a.hlcCounter !== b.hlcCounter) {
    return a.hlcCounter < b.hlcCounter ? -1 : 1;
  }

  if (a.hlcDeviceId === b.hlcDeviceId) {
    return 0;
  }

  return a.hlcDeviceId < b.hlcDeviceId ? -1 : 1;
}

export function clonePendingOperation<S extends CollectionValueMap>(
  operation: PendingOperation<S>,
): PendingOperation<S> {
  if (operation.type === "put") {
    return {
      ...operation,
      data: structuredClone(operation.data),
    };
  }

  return { ...operation };
}

export function appendPendingOperations<S extends CollectionValueMap>(
  destination: PendingOperation<S>[],
  operations: ReadonlyArray<PendingOperation<S>>,
): void {
  if (operations.length === 0) {
    return;
  }

  for (const operation of operations) {
    destination.push(clonePendingOperation(operation));
  }

  destination.sort((a, b) => a.sequence - b.sequence);
}

export function getPendingOperations<S extends CollectionValueMap>(
  operations: ReadonlyArray<PendingOperation<S>>,
  limit: number,
): Array<PendingOperation<S>> {
  return operations
    .slice(0, Math.max(0, limit))
    .map((operation) => clonePendingOperation(operation));
}

export function removePendingOperationsThrough<S extends CollectionValueMap>(
  operations: ReadonlyArray<PendingOperation<S>>,
  sequenceInclusive: PendingSequence,
): Array<PendingOperation<S>> {
  return operations.filter((operation) => operation.sequence > sequenceInclusive);
}
