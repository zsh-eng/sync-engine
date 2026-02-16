import { describe, expect, test } from "bun:test";

import {
  createConnectionManager,
  deriveSyncAbility,
  type ConnectionManager,
} from "./connection-manager";
import type { ConnectionState } from "./types";

function createTestManager(
  input: Parameters<typeof createConnectionManager>[0] = {},
): ConnectionManager {
  return createConnectionManager({
    now: () => 10_000,
    ...input,
  });
}

describe("createConnectionManager", () => {
  describe("initial state", () => {
    test("defaults to online, unauthenticated, visible", () => {
      const manager = createTestManager();
      expect(manager.getState()).toEqual({
        network: "online",
        auth: "unauthenticated",
        visibility: "visible",
      });
    });

    test("respects custom initial values", () => {
      const manager = createTestManager({
        initialNetwork: "offline",
        initialAuth: "authenticated",
        initialVisibility: "hidden",
      });
      expect(manager.getState()).toEqual({
        network: "offline",
        auth: "authenticated",
        visibility: "hidden",
      });
    });
  });

  describe("network transitions", () => {
    test("setOffline changes network to offline", () => {
      const manager = createTestManager();
      manager.setOffline();
      expect(manager.getState().network).toBe("offline");
    });

    test("setOnline changes network to online", () => {
      const manager = createTestManager({ initialNetwork: "offline" });
      manager.setOnline();
      expect(manager.getState().network).toBe("online");
    });
  });

  describe("auth transitions", () => {
    test("setAuthenticated changes auth to authenticated", () => {
      const manager = createTestManager();
      manager.setAuthenticated();
      expect(manager.getState().auth).toBe("authenticated");
    });

    test("setUnauthenticated changes auth to unauthenticated", () => {
      const manager = createTestManager({ initialAuth: "authenticated" });
      manager.setUnauthenticated();
      expect(manager.getState().auth).toBe("unauthenticated");
    });

    test("setAuthExpired changes auth to expired", () => {
      const manager = createTestManager({ initialAuth: "authenticated" });
      manager.setAuthExpired();
      expect(manager.getState().auth).toBe("expired");
    });
  });

  describe("visibility transitions", () => {
    test("setHidden changes visibility to hidden", () => {
      const manager = createTestManager();
      manager.setHidden();
      expect(manager.getState().visibility).toBe("hidden");
    });

    test("setVisible changes visibility to visible", () => {
      const manager = createTestManager({ initialVisibility: "hidden" });
      manager.setVisible();
      expect(manager.getState().visibility).toBe("visible");
    });
  });

  describe("idempotent transitions", () => {
    test("setOnline when already online does not notify", () => {
      const manager = createTestManager();
      const calls: ConnectionState[] = [];
      manager.subscribe((state) => calls.push(state));

      manager.setOnline();
      expect(calls).toHaveLength(0);
    });

    test("setOffline when already offline does not notify", () => {
      const manager = createTestManager({ initialNetwork: "offline" });
      const calls: ConnectionState[] = [];
      manager.subscribe((state) => calls.push(state));

      manager.setOffline();
      expect(calls).toHaveLength(0);
    });

    test("setAuthenticated when already authenticated does not notify", () => {
      const manager = createTestManager({ initialAuth: "authenticated" });
      const calls: ConnectionState[] = [];
      manager.subscribe((state) => calls.push(state));

      manager.setAuthenticated();
      expect(calls).toHaveLength(0);
    });

    test("setVisible when already visible does not notify", () => {
      const manager = createTestManager();
      const calls: ConnectionState[] = [];
      manager.subscribe((state) => calls.push(state));

      manager.setVisible();
      expect(calls).toHaveLength(0);
    });
  });

  describe("subscriber notifications", () => {
    test("listener receives current and previous state", () => {
      const manager = createTestManager();
      const calls: Array<{ state: ConnectionState; previous: ConnectionState }> = [];
      manager.subscribe((state, previous) => calls.push({ state, previous }));

      manager.setOffline();

      expect(calls).toHaveLength(1);
      expect(calls[0]!.previous).toEqual({
        network: "online",
        auth: "unauthenticated",
        visibility: "visible",
      });
      expect(calls[0]!.state).toEqual({
        network: "offline",
        auth: "unauthenticated",
        visibility: "visible",
      });
    });

    test("multiple subscribers all get notified", () => {
      const manager = createTestManager();
      const calls1: ConnectionState[] = [];
      const calls2: ConnectionState[] = [];
      manager.subscribe((state) => calls1.push(state));
      manager.subscribe((state) => calls2.push(state));

      manager.setOffline();

      expect(calls1).toHaveLength(1);
      expect(calls2).toHaveLength(1);
    });

    test("unsubscribe stops notifications", () => {
      const manager = createTestManager();
      const calls: ConnectionState[] = [];
      const unsub = manager.subscribe((state) => calls.push(state));

      manager.setOffline();
      expect(calls).toHaveLength(1);

      unsub();
      manager.setOnline();
      expect(calls).toHaveLength(1);
    });

    test("only changed signal triggers notification", () => {
      const manager = createTestManager({ initialAuth: "authenticated" });
      const calls: Array<{ state: ConnectionState; previous: ConnectionState }> = [];
      manager.subscribe((state, previous) => calls.push({ state, previous }));

      manager.setHidden();
      expect(calls).toHaveLength(1);
      expect(calls[0]!.state.visibility).toBe("hidden");
      expect(calls[0]!.state.auth).toBe("authenticated");
      expect(calls[0]!.state.network).toBe("online");
    });
  });

  describe("getSyncAbility", () => {
    test("returns can-sync when online, authenticated, visible", () => {
      const manager = createTestManager({ initialAuth: "authenticated" });
      expect(manager.getSyncAbility()).toBe("can-sync");
    });

    test("returns no-network when offline", () => {
      const manager = createTestManager({
        initialNetwork: "offline",
        initialAuth: "authenticated",
      });
      expect(manager.getSyncAbility()).toBe("no-network");
    });

    test("returns needs-auth when unauthenticated", () => {
      const manager = createTestManager();
      expect(manager.getSyncAbility()).toBe("needs-auth");
    });

    test("returns needs-auth when expired", () => {
      const manager = createTestManager({ initialAuth: "expired" });
      expect(manager.getSyncAbility()).toBe("needs-auth");
    });

    test("returns paused when hidden but otherwise ready", () => {
      const manager = createTestManager({
        initialAuth: "authenticated",
        initialVisibility: "hidden",
      });
      expect(manager.getSyncAbility()).toBe("paused");
    });

    test("reflects state changes", () => {
      const manager = createTestManager();
      expect(manager.getSyncAbility()).toBe("needs-auth");

      manager.setAuthenticated();
      expect(manager.getSyncAbility()).toBe("can-sync");

      manager.setOffline();
      expect(manager.getSyncAbility()).toBe("no-network");

      manager.setOnline();
      expect(manager.getSyncAbility()).toBe("can-sync");

      manager.setHidden();
      expect(manager.getSyncAbility()).toBe("paused");
    });
  });

  describe("backoff", () => {
    test("initial backoff has zero failures and null nextRetryAtMs", () => {
      const manager = createTestManager();
      expect(manager.getBackoff()).toEqual({
        consecutiveFailures: 0,
        nextRetryAtMs: null,
      });
    });

    test("recordFailure increments failures and sets nextRetryAtMs", () => {
      const manager = createTestManager();
      manager.recordFailure();
      const b = manager.getBackoff();
      expect(b.consecutiveFailures).toBe(1);
      expect(b.nextRetryAtMs).toBeGreaterThan(10_000);
    });

    test("consecutive failures increase delay exponentially", () => {
      const manager = createTestManager({
        backoff: { baseMs: 1000, multiplier: 2, maxMs: 60_000 },
      });

      manager.recordFailure(); // 1st: base = 1000
      const b1 = manager.getBackoff();

      manager.recordFailure(); // 2nd: base * 2 = 2000
      const b2 = manager.getBackoff();

      manager.recordFailure(); // 3rd: base * 4 = 4000
      const b3 = manager.getBackoff();

      expect(b2.nextRetryAtMs!).toBeGreaterThan(b1.nextRetryAtMs!);
      expect(b3.nextRetryAtMs!).toBeGreaterThan(b2.nextRetryAtMs!);
    });

    test("delay is capped at maxMs", () => {
      const manager = createTestManager({
        backoff: { baseMs: 1000, multiplier: 10, maxMs: 5_000 },
      });

      // 1st failure: min(1000 * 10^0, 5000) = 1000 + jitter
      manager.recordFailure();
      // 2nd failure: min(1000 * 10^1, 5000) = 5000 + jitter
      manager.recordFailure();
      // 3rd failure: min(1000 * 10^2, 5000) = 5000 + jitter (capped)
      manager.recordFailure();

      const b = manager.getBackoff();
      // nextRetryAtMs = now(10_000) + delay(5000) + jitter(±1250)
      // So it should be between 10_000 + 3750 and 10_000 + 6250
      expect(b.nextRetryAtMs!).toBeGreaterThanOrEqual(10_000 + 3_750);
      expect(b.nextRetryAtMs!).toBeLessThanOrEqual(10_000 + 6_250);
    });

    test("recordSuccess resets backoff", () => {
      const manager = createTestManager();
      manager.recordFailure();
      manager.recordFailure();

      manager.recordSuccess();
      expect(manager.getBackoff()).toEqual({
        consecutiveFailures: 0,
        nextRetryAtMs: null,
      });
    });

    test("backoff changes do NOT trigger connection state listeners", () => {
      const manager = createTestManager();
      const calls: ConnectionState[] = [];
      manager.subscribe((state) => calls.push(state));

      manager.recordFailure();
      manager.recordFailure();
      manager.recordSuccess();

      expect(calls).toHaveLength(0);
    });
  });
});

describe("deriveSyncAbility", () => {
  test("offline trumps all other states", () => {
    expect(
      deriveSyncAbility({ network: "offline", auth: "authenticated", visibility: "visible" }),
    ).toBe("no-network");
    expect(
      deriveSyncAbility({ network: "offline", auth: "unauthenticated", visibility: "hidden" }),
    ).toBe("no-network");
  });

  test("needs-auth trumps paused", () => {
    expect(
      deriveSyncAbility({ network: "online", auth: "unauthenticated", visibility: "hidden" }),
    ).toBe("needs-auth");
    expect(deriveSyncAbility({ network: "online", auth: "expired", visibility: "hidden" })).toBe(
      "needs-auth",
    );
  });

  test("paused when hidden but otherwise ready", () => {
    expect(
      deriveSyncAbility({ network: "online", auth: "authenticated", visibility: "hidden" }),
    ).toBe("paused");
  });

  test("can-sync when all signals green", () => {
    expect(
      deriveSyncAbility({ network: "online", auth: "authenticated", visibility: "visible" }),
    ).toBe("can-sync");
  });
});
