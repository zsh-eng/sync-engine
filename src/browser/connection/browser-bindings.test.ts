import { describe, expect, test } from "bun:test";

import { createBrowserConnectionDriver } from "./browser-bindings";
import { createConnectionManager } from "../../core/connection";
import type { ConnectionState } from "../../core/types";

function createMockWindow(opts: { online?: boolean; hidden?: boolean } = {}) {
  const listeners: Record<string, Set<EventListener>> = {};
  const docListeners: Record<string, Set<EventListener>> = {};

  const win = {
    navigator: { onLine: opts.online ?? true },
    document: {
      hidden: opts.hidden ?? false,
      addEventListener(event: string, handler: EventListener) {
        if (!docListeners[event]) docListeners[event] = new Set();
        docListeners[event].add(handler);
      },
      removeEventListener(event: string, handler: EventListener) {
        docListeners[event]?.delete(handler);
      },
    },
    addEventListener(event: string, handler: EventListener) {
      if (!listeners[event]) listeners[event] = new Set();
      listeners[event].add(handler);
    },
    removeEventListener(event: string, handler: EventListener) {
      listeners[event]?.delete(handler);
    },
  } as unknown as Window;

  function dispatch(event: string) {
    for (const handler of listeners[event] ?? []) {
      handler(new Event(event));
    }
  }

  function dispatchDoc(event: string) {
    for (const handler of docListeners[event] ?? []) {
      handler(new Event(event));
    }
  }

  return { win, dispatch, dispatchDoc, docListeners, listeners };
}

describe("createBrowserConnectionDriver", () => {
  function subscribeToDriver(driver: ReturnType<typeof createBrowserConnectionDriver>): {
    states: ConnectionState[];
    unsubscribe: () => void;
  } {
    const states: ConnectionState[] = [];
    const unsubscribe = driver.subscribe((state) => states.push(state));
    return { states, unsubscribe };
  }

  test("emits initial connected when online and visible", () => {
    const { win } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    expect(states).toEqual(["connected"]);
    unsubscribe();
  });

  test("emits initial offline when navigator is offline", () => {
    const { win } = createMockWindow({ online: false, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    expect(states).toEqual(["offline"]);
    unsubscribe();
  });

  test("emits initial paused when online and hidden", () => {
    const { win } = createMockWindow({ online: true, hidden: true });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    expect(states).toEqual(["paused"]);
    unsubscribe();
  });

  test("offline event transitions to offline", () => {
    const { win, dispatch } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    (win.navigator as { onLine: boolean }).onLine = false;
    dispatch("offline");

    expect(states).toEqual(["connected", "offline"]);
    unsubscribe();
  });

  test("online event transitions from offline to connected", () => {
    const { win, dispatch } = createMockWindow({ online: false, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    (win.navigator as { onLine: boolean }).onLine = true;
    dispatch("online");

    expect(states).toEqual(["offline", "connected"]);
    unsubscribe();
  });

  test("visibilitychange transitions between connected and paused", () => {
    const { win, dispatchDoc } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    (win.document as { hidden: boolean }).hidden = true;
    dispatchDoc("visibilitychange");
    (win.document as { hidden: boolean }).hidden = false;
    dispatchDoc("visibilitychange");

    expect(states).toEqual(["connected", "paused", "connected"]);
    unsubscribe();
  });

  test("does not emit duplicate state changes", () => {
    const { win, dispatch } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const { states, unsubscribe } = subscribeToDriver(driver);

    dispatch("online");
    dispatch("online");

    expect(states).toEqual(["connected"]);
    unsubscribe();
  });

  test("unsubscribe removes all browser listeners", () => {
    const { win, listeners, docListeners } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const unsubscribe = driver.subscribe(() => {});

    expect(listeners["online"]?.size).toBe(1);
    expect(listeners["offline"]?.size).toBe(1);
    expect(docListeners["visibilitychange"]?.size).toBe(1);

    unsubscribe();

    expect(listeners["online"]?.size).toBe(0);
    expect(listeners["offline"]?.size).toBe(0);
    expect(docListeners["visibilitychange"]?.size).toBe(0);
  });

  test("works with createConnectionManager integration", () => {
    const { win, dispatchDoc } = createMockWindow({ online: true, hidden: false });
    const driver = createBrowserConnectionDriver({ window: win });
    const manager = createConnectionManager({ driver });

    expect(manager.getState()).toBe("connected");

    (win.document as { hidden: boolean }).hidden = true;
    dispatchDoc("visibilitychange");

    expect(manager.getState()).toBe("paused");
  });
});
