import { describe, expect, test } from "bun:test";

import {
  compareClocks,
  createClockService,
  formatClock,
  nextClock,
  nextClockFromRemote,
  parseClock,
  type ClockStorageAdapter,
  type HybridLogicalClock,
} from "./index";

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

describe("HLC stateless functions", () => {
  test("format/parse roundtrip", () => {
    const clock = formatClock({ wallMs: 100, counter: 2, nodeId: "nodeA" });
    expect(clock).toBe("100-2-nodeA");
    expect(parseClock(clock)).toEqual({ wallMs: 100, counter: 2, nodeId: "nodeA" });
  });

  test("compareClocks uses wall, then counter, then nodeId", () => {
    expect(compareClocks(asClock("100-0-a"), asClock("101-0-a"))).toBe(-1);
    expect(compareClocks(asClock("100-1-a"), asClock("100-0-a"))).toBe(1);
    expect(compareClocks(asClock("100-1-a"), asClock("100-1-b"))).toBe(-1);
    expect(compareClocks(asClock("100-1-a"), asClock("100-1-a"))).toBe(0);
  });

  test("compareClocks treats counter as numeric and not lexicographic", () => {
    expect(compareClocks(asClock("100-2-nodeA"), asClock("100-10-nodeB"))).toBe(-1);
  });

  test("nextClock starts from now with counter zero", () => {
    const clock = nextClock({ nodeId: "local", nowMs: 50 });
    expect(clock).toBe("50-0-local");
  });

  test("nextClock increments counter on same wall time", () => {
    const clock = nextClock({
      lastClock: asClock("50-3-local"),
      nodeId: "local",
      nowMs: 50,
    });

    expect(clock).toBe("50-4-local");
  });

  test("nextClock increments counter when local wall is ahead of now", () => {
    const clock = nextClock({
      lastClock: asClock("70-2-local"),
      nodeId: "local",
      nowMs: 60,
    });

    expect(clock).toBe("70-3-local");
  });

  test("nextClock resets counter when physical clock moves forward", () => {
    const clock = nextClock({
      lastClock: asClock("70-2-local"),
      nodeId: "local",
      nowMs: 99,
    });

    expect(clock).toBe("99-0-local");
  });

  test("nextClockFromRemote advances from remote when remote dominates", () => {
    const clock = nextClockFromRemote({
      lastLocalClock: asClock("100-2-local"),
      remoteClock: asClock("110-4-remote"),
      nodeId: "local",
      nowMs: 105,
    });

    expect(clock).toBe("110-5-local");
  });

  test("nextClockFromRemote advances from local when local dominates", () => {
    const clock = nextClockFromRemote({
      lastLocalClock: asClock("120-4-local"),
      remoteClock: asClock("110-9-remote"),
      nodeId: "local",
      nowMs: 115,
    });

    expect(clock).toBe("120-5-local");
  });

  test("nextClockFromRemote takes max counter when local and remote walls tie", () => {
    const clock = nextClockFromRemote({
      lastLocalClock: asClock("120-4-local"),
      remoteClock: asClock("120-7-remote"),
      nodeId: "local",
      nowMs: 118,
    });

    expect(clock).toBe("120-8-local");
  });

  test("nextClockFromRemote uses now when now dominates", () => {
    const clock = nextClockFromRemote({
      lastLocalClock: asClock("120-4-local"),
      remoteClock: asClock("121-7-remote"),
      nodeId: "local",
      nowMs: 150,
    });

    expect(clock).toBe("150-0-local");
  });

  test("nextClockFromRemote works with no prior local clock", () => {
    const clock = nextClockFromRemote({
      remoteClock: asClock("42-8-remote"),
      nodeId: "local",
      nowMs: 40,
    });

    expect(clock).toBe("42-9-local");
  });
});

describe("HLC service", () => {
  test("next persists generated clocks and peek reads current value", async () => {
    let stored: HybridLogicalClock | undefined;
    let writes = 0;

    const storage: ClockStorageAdapter = {
      read: () => stored,
      write: (clock) => {
        stored = clock;
        writes += 1;
      },
    };

    const service = createClockService({ nodeId: "local", storage, now: () => 100 });

    expect(await service.peek()).toBeUndefined();
    expect(await service.next()).toBe("100-0-local");
    expect(await service.next()).toBe("100-1-local");
    expect(await service.peek()).toBe("100-1-local");
    expect(stored).toBe("100-1-local");
    expect(writes).toBe(2);
  });

  test("nextFromRemote merges with persisted state across service instances", async () => {
    let stored: HybridLogicalClock | undefined;

    const storage: ClockStorageAdapter = {
      read: () => stored,
      write: (clock) => {
        stored = clock;
      },
    };

    const first = createClockService({ nodeId: "local", storage, now: () => 100 });
    expect(await first.next()).toBe("100-0-local");

    const second = createClockService({ nodeId: "local", storage, now: () => 120 });
    const merged = await second.nextFromRemote(asClock("150-4-remote"));

    expect(merged).toBe("150-5-local");
    expect(await second.peek()).toBe("150-5-local");
  });

  test("nextBatch returns the next N clocks and persists the latest", async () => {
    let stored: HybridLogicalClock | undefined;
    let writes = 0;

    const storage: ClockStorageAdapter = {
      read: () => stored,
      write: (clock) => {
        stored = clock;
        writes += 1;
      },
    };

    const service = createClockService({ nodeId: "local", storage, now: () => 300 });

    const batch = await service.nextBatch(4);
    expect(batch).toEqual(["300-0-local", "300-1-local", "300-2-local", "300-3-local"]);
    expect(await service.peek()).toBe("300-3-local");
    expect(stored).toBe("300-3-local");
    expect(writes).toBe(1);
  });

  test("nextBatch validates count", async () => {
    const service = createClockService({
      nodeId: "local",
      storage: {
        read: () => undefined,
        write: () => undefined,
      },
      now: () => 1,
    });

    await expect(service.nextBatch(0)).rejects.toThrow("Invalid count");
  });

  test("concurrent calls stay monotonic", async () => {
    let stored: HybridLogicalClock | undefined;

    const storage: ClockStorageAdapter = {
      read: () => stored,
      write: async (clock) => {
        await Promise.resolve();
        stored = clock;
      },
    };

    const service = createClockService({ nodeId: "local", storage, now: () => 200 });

    const results = await Promise.all([
      service.next(),
      service.next(),
      service.next(),
      service.next(),
      service.next(),
    ]);

    expect(results).toEqual([
      "200-0-local",
      "200-1-local",
      "200-2-local",
      "200-3-local",
      "200-4-local",
    ]);
    expect(await service.peek()).toBe("200-4-local");
  });
});
