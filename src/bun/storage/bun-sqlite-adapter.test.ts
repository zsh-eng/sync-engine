import { describe, expect, test } from "bun:test";

import { createEngine } from "../../core/engine";
import {
  createClockService,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "../../core/hlc";
import type { AnyStoredRow } from "../../core/types";
import { createBunSqliteRowStoreAdapter } from "./bun-sqlite-adapter";

interface RowValue {
  title: string;
}

interface Collections {
  books: RowValue;
}

const TEST_NAMESPACE = "books-app";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

function remoteRow(
  clock: HybridLogicalClock,
  title: string,
  txId: string,
): AnyStoredRow<Collections> {
  const parsed = parseClock(clock);
  return {
    namespace: TEST_NAMESPACE,
    collectionId: "books",
    id: "book-1",
    parentId: null,
    data: { title },
    txId,
    tombstone: false,
    committedTimestampMs: parsed.wallMs,
    hlcTimestampMs: parsed.wallMs,
    hlcCounter: parsed.counter,
    hlcDeviceId: parsed.nodeId,
  };
}

function createBunSqliteEngine() {
  let storedClock: HybridLogicalClock | undefined;
  let txCounter = 0;

  const clockStorage: ClockStorageAdapter = {
    read: () => storedClock,
    write: (clock) => {
      storedClock = clock;
    },
  };

  const clock = createClockService({
    nodeId: "deviceA",
    storage: clockStorage,
    now: () => 3_000,
  });

  const adapter = createBunSqliteRowStoreAdapter<Collections>({
    userID: "user-1",
    namespace: TEST_NAMESPACE,
  });
  const engine = createEngine<Collections>({
    adapter,
    clock,
    namespace: TEST_NAMESPACE,
    txIDFactory: () => `tx_${++txCounter}`,
  });

  return {
    adapter,
    engine,
    cleanup: () => {
      adapter.close();
    },
  };
}

describe("BunSqliteRowStoreAdapter", () => {
  test("reports write outcomes when the same row is updated twice", async () => {
    const { engine, cleanup } = createBunSqliteEngine();

    try {
      const write = await engine.batchLocal([
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune" } },
        { type: "put", collectionId: "books", id: "book-1", data: { title: "Dune Messiah" } },
      ]);

      expect(write[0]).toMatchObject({
        collectionId: "books",
        id: "book-1",
        hlcTimestampMs: 3000,
        hlcCounter: 0,
        applied: true,
      });
      expect(write[1]).toMatchObject({
        collectionId: "books",
        id: "book-1",
        hlcTimestampMs: 3000,
        hlcCounter: 1,
        applied: true,
      });

      const read = await engine.get("books", "book-1");
      expect(read).toMatchObject({
        data: { title: "Dune Messiah" },
        txId: "tx_1",
        tombstone: false,
      });
    } finally {
      cleanup();
    }
  });

  test("stores rows and preserves row metadata", async () => {
    const { adapter, engine, cleanup } = createBunSqliteEngine();

    try {
      await engine.put("books", "book-1", { title: "Dune" });
      await engine.delete("books", "book-1");

      const raw = await adapter.getRawRow("books", "book-1");
      expect(raw).toMatchObject({
        committed_timestamp_ms: 3000,
        hlc_wall_ms: 3000,
        hlc_counter: 1,
        hlc_node_id: "deviceA",
        tombstone: 1,
      });
    } finally {
      cleanup();
    }
  });

  test("applies LWW tie-break with node ID for equal wall/counter", async () => {
    const { engine, cleanup } = createBunSqliteEngine();

    try {
      const firstApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceA"), "from A", "tx_a"),
      ]);
      expect(firstApply.appliedCount).toBe(1);

      const secondApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceZ"), "from Z", "tx_z"),
      ]);
      expect(secondApply.appliedCount).toBe(1);

      const staleApply = await engine.applyRemote([
        remoteRow(asClock("9000-2-deviceB"), "from B", "tx_b"),
      ]);
      expect(staleApply.appliedCount).toBe(0);

      const read = await engine.get("books", "book-1");
      expect(read).toMatchObject({
        data: { title: "from Z" },
        txId: "tx_z",
      });
    } finally {
      cleanup();
    }
  });
});
