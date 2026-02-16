import { parseClock, type HybridLogicalClock } from "./clock";
import type { ClockStorageAdapter } from "./service";

export interface LocalStorageClockAdapterOptions {
  storage?: Storage;
  key?: string;
}

const DEFAULT_STORAGE_KEY = "sync-engine:hlc";

function resolveStorage(storage?: Storage): Storage {
  if (storage) {
    return storage;
  }

  if (typeof globalThis.localStorage !== "undefined") {
    return globalThis.localStorage;
  }

  throw new Error("localStorage is not available in this environment");
}

export function createLocalStorageClockAdapter(
  options: LocalStorageClockAdapterOptions = {},
): ClockStorageAdapter {
  const storage = resolveStorage(options.storage);
  const key = options.key ?? DEFAULT_STORAGE_KEY;

  return {
    read(): HybridLogicalClock | undefined {
      const value = storage.getItem(key);
      if (value === null) {
        return undefined;
      }

      try {
        parseClock(value as HybridLogicalClock);
      } catch {
        throw new Error(`Invalid HLC stored at localStorage key: ${key}`);
      }

      return value as HybridLogicalClock;
    },

    write(clock: HybridLogicalClock): void {
      storage.setItem(key, clock);
    },
  };
}
