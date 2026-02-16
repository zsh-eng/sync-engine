import type { HybridLogicalClock } from "./clock";
import { nextClock, nextClockFromRemote } from "./clock";

export type MaybePromise<T> = T | Promise<T>;

export interface ClockStorageAdapter {
  read(): MaybePromise<HybridLogicalClock | undefined>;
  write(clock: HybridLogicalClock): MaybePromise<void>;
}

export interface CreateClockServiceInput {
  nodeId: string;
  storage: ClockStorageAdapter;
  now?: () => number;
}

export interface ClockService {
  peek(): Promise<HybridLogicalClock | undefined>;
  next(nowMs?: number): Promise<HybridLogicalClock>;
  nextBatch(count: number, nowMs?: number): Promise<HybridLogicalClock[]>;
  nextFromRemote(remoteClock: HybridLogicalClock, nowMs?: number): Promise<HybridLogicalClock>;
}

export function createClockService(input: CreateClockServiceInput): ClockService {
  const now = input.now ?? Date.now;
  let current: HybridLogicalClock | undefined;
  let hasLoaded = false;

  // Serialize operations to prevent clock regressions from concurrent callers.
  let queue: Promise<void> = Promise.resolve();

  async function load(): Promise<HybridLogicalClock | undefined> {
    if (!hasLoaded) {
      current = await input.storage.read();
      hasLoaded = true;
    }

    return current;
  }

  async function persist(clock: HybridLogicalClock): Promise<void> {
    current = clock;
    await input.storage.write(clock);
  }

  async function enqueue<T>(operation: () => Promise<T>): Promise<T> {
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

  return {
    async peek(): Promise<HybridLogicalClock | undefined> {
      return load();
    },

    async next(nowMs?: number): Promise<HybridLogicalClock> {
      return enqueue(async () => {
        const last = await load();
        const clock = nextClock({
          lastClock: last,
          nodeId: input.nodeId,
          nowMs: nowMs ?? now(),
        });
        await persist(clock);
        return clock;
      });
    },

    async nextBatch(count: number, nowMs?: number): Promise<HybridLogicalClock[]> {
      if (!Number.isInteger(count) || count <= 0) {
        throw new Error(`Invalid count: expected a positive integer, received ${count}`);
      }

      return enqueue(async () => {
        const baseNowMs = nowMs ?? now();
        const clocks: HybridLogicalClock[] = [];
        let last = await load();
        let finalClock: HybridLogicalClock | undefined;

        for (let index = 0; index < count; index += 1) {
          const clock = nextClock({
            lastClock: last,
            nodeId: input.nodeId,
            nowMs: baseNowMs,
          });
          clocks.push(clock);
          last = clock;
          finalClock = clock;
        }

        if (!finalClock) {
          throw new Error("Unable to generate HLC batch");
        }

        await persist(finalClock);
        return clocks;
      });
    },

    async nextFromRemote(
      remoteClock: HybridLogicalClock,
      nowMs?: number,
    ): Promise<HybridLogicalClock> {
      return enqueue(async () => {
        const last = await load();
        const clock = nextClockFromRemote({
          lastLocalClock: last,
          remoteClock,
          nodeId: input.nodeId,
          nowMs: nowMs ?? now(),
        });
        await persist(clock);
        return clock;
      });
    },
  };
}
