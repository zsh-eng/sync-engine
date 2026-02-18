import { describe, expect, test } from "bun:test";

import { createClockService, type HybridLogicalClock } from "../../core/hlc";
import { createLocalStorageClockAdapter } from "./local-storage-adapter";

class MemoryStorage implements Storage {
  #data = new Map<string, string>();

  get length(): number {
    return this.#data.size;
  }

  clear(): void {
    this.#data.clear();
  }

  getItem(key: string): string | null {
    return this.#data.get(key) ?? null;
  }

  key(index: number): string | null {
    return Array.from(this.#data.keys())[index] ?? null;
  }

  removeItem(key: string): void {
    this.#data.delete(key);
  }

  setItem(key: string, value: string): void {
    this.#data.set(key, value);
  }
}

function asClock(value: string): HybridLogicalClock {
  return value as HybridLogicalClock;
}

describe("localStorage clock adapter", () => {
  test("read returns undefined when key is missing", async () => {
    const storage = new MemoryStorage();
    const adapter = createLocalStorageClockAdapter({ storage, key: "hlc" });

    expect(await adapter.read()).toBeUndefined();
  });

  test("write persists and read returns the stored clock", async () => {
    const storage = new MemoryStorage();
    const adapter = createLocalStorageClockAdapter({ storage, key: "hlc" });

    await adapter.write(asClock("100-2-nodeA"));
    expect(await adapter.read()).toBe(asClock("100-2-nodeA"));
  });

  test("read rejects invalid stored values", async () => {
    const storage = new MemoryStorage();
    storage.setItem("hlc", "not-a-valid-hlc");
    const adapter = createLocalStorageClockAdapter({ storage, key: "hlc" });

    expect(() => adapter.read()).toThrow("Invalid HLC stored");
  });

  test("works with clock service for persisted progression", async () => {
    const storage = new MemoryStorage();
    const adapter = createLocalStorageClockAdapter({ storage, key: "hlc" });

    const firstService = createClockService({
      nodeId: "local",
      storage: adapter,
      now: () => 100,
    });

    expect(await firstService.next()).toBe(asClock("100-0-local"));
    expect(await firstService.next()).toBe(asClock("100-1-local"));

    const secondService = createClockService({
      nodeId: "local",
      storage: adapter,
      now: () => 120,
    });

    expect(await secondService.next()).toBe(asClock("120-0-local"));
    expect(storage.getItem("hlc")).toBe("120-0-local");
  });
});
