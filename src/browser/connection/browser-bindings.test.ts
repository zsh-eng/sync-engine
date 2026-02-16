import { describe, expect, test } from "bun:test";

import { bindBrowserEvents } from "./browser-bindings";
import { createConnectionManager } from "../../core/connection/connection-manager";

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

describe("bindBrowserEvents", () => {
  test("dispatching offline event calls setOffline", () => {
    const manager = createConnectionManager();
    const { win, dispatch } = createMockWindow();
    bindBrowserEvents({ manager, window: win });

    dispatch("offline");
    expect(manager.getState().network).toBe("offline");
  });

  test("dispatching online event calls setOnline", () => {
    const manager = createConnectionManager({ initialNetwork: "offline" });
    const { win, dispatch } = createMockWindow();
    bindBrowserEvents({ manager, window: win });

    dispatch("online");
    expect(manager.getState().network).toBe("online");
  });

  test("syncs initial navigator.onLine = false at bind time", () => {
    const manager = createConnectionManager();
    const { win } = createMockWindow({ online: false });
    bindBrowserEvents({ manager, window: win });

    expect(manager.getState().network).toBe("offline");
  });

  test("does not change network if navigator.onLine = true at bind time", () => {
    const manager = createConnectionManager();
    const calls: unknown[] = [];
    manager.subscribe((state) => calls.push(state));

    const { win } = createMockWindow({ online: true });
    bindBrowserEvents({ manager, window: win });

    // No notification because initial state is already online
    expect(calls).toHaveLength(0);
    expect(manager.getState().network).toBe("online");
  });

  test("syncs initial document.hidden = true at bind time", () => {
    const manager = createConnectionManager();
    const { win } = createMockWindow({ hidden: true });
    bindBrowserEvents({ manager, window: win });

    expect(manager.getState().visibility).toBe("hidden");
  });

  test("visibilitychange with document.hidden = true calls setHidden", () => {
    const manager = createConnectionManager();
    const { win, dispatchDoc } = createMockWindow();
    bindBrowserEvents({ manager, window: win });

    (win.document as { hidden: boolean }).hidden = true;
    dispatchDoc("visibilitychange");
    expect(manager.getState().visibility).toBe("hidden");
  });

  test("visibilitychange with document.hidden = false calls setVisible", () => {
    const manager = createConnectionManager({ initialVisibility: "hidden" });
    const { win, dispatchDoc } = createMockWindow({ hidden: true });
    bindBrowserEvents({ manager, window: win });

    (win.document as { hidden: boolean }).hidden = false;
    dispatchDoc("visibilitychange");
    expect(manager.getState().visibility).toBe("visible");
  });

  test("cleanup removes all event listeners", () => {
    const manager = createConnectionManager();
    const { win, dispatch, dispatchDoc, listeners, docListeners } = createMockWindow();
    const cleanup = bindBrowserEvents({ manager, window: win });

    // Verify listeners were added
    expect(listeners["online"]?.size).toBe(1);
    expect(listeners["offline"]?.size).toBe(1);
    expect(docListeners["visibilitychange"]?.size).toBe(1);

    cleanup();

    // Verify listeners were removed
    expect(listeners["online"]?.size).toBe(0);
    expect(listeners["offline"]?.size).toBe(0);
    expect(docListeners["visibilitychange"]?.size).toBe(0);

    // Events after cleanup have no effect
    dispatch("offline");
    expect(manager.getState().network).toBe("online");
  });
});
