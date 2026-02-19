import { describe, expect, test } from "bun:test";

import { createConnectionManager } from "./connection-manager";
import type { ConnectionDriver, ConnectionState } from "../types";

function createDriver(initialState?: ConnectionState): {
  driver: ConnectionDriver;
  emit: (state: ConnectionState) => void;
} {
  const listeners = new Set<(state: ConnectionState) => void>();

  return {
    driver: {
      subscribe(listener: (state: ConnectionState) => void): () => void {
        listeners.add(listener);
        if (initialState !== undefined) {
          listener(initialState);
        }

        return () => {
          listeners.delete(listener);
        };
      },
    },
    emit(state: ConnectionState): void {
      for (const listener of listeners) {
        listener(state);
      }
    },
  };
}

describe("createConnectionManager", () => {
  test("defaults to offline before the driver emits any state", () => {
    const { driver } = createDriver();
    const manager = createConnectionManager({ driver });
    expect(manager.getState()).toBe("offline");
  });

  test("respects custom initial state before driver emissions", () => {
    const { driver } = createDriver();
    const manager = createConnectionManager({
      driver,
      initialState: "paused",
    });

    expect(manager.getState()).toBe("paused");
  });

  test("tracks driver state", () => {
    const { driver, emit } = createDriver("connected");
    const manager = createConnectionManager({ driver });

    expect(manager.getState()).toBe("connected");

    emit("needsAuth");
    expect(manager.getState()).toBe("needsAuth");

    emit("paused");
    expect(manager.getState()).toBe("paused");
  });

  test("notifies listeners when state changes", () => {
    const { driver, emit } = createDriver("offline");
    const manager = createConnectionManager({ driver });
    const calls: ConnectionState[] = [];

    manager.subscribe((state) => calls.push(state));
    emit("connected");
    emit("needsAuth");

    expect(calls).toEqual(["connected", "needsAuth"]);
  });

  test("does not notify listeners when state does not change", () => {
    const { driver, emit } = createDriver("offline");
    const manager = createConnectionManager({ driver });
    const calls: ConnectionState[] = [];

    manager.subscribe((state) => calls.push(state));
    emit("offline");
    emit("offline");

    expect(calls).toHaveLength(0);
  });

  test("unsubscribe stops notifications", () => {
    const { driver, emit } = createDriver("offline");
    const manager = createConnectionManager({ driver });
    const calls: ConnectionState[] = [];

    const unsubscribe = manager.subscribe((state) => calls.push(state));
    emit("connected");
    expect(calls).toEqual(["connected"]);

    unsubscribe();
    emit("needsAuth");
    expect(calls).toEqual(["connected"]);
  });
});
