import type { ConnectionDriver, ConnectionState } from "../../core/types";

export interface CreateBrowserConnectionDriverInput {
  /** Injectable window for testing. Defaults to globalThis.window. */
  window?: Window;
}

/**
 * Build a browser ConnectionDriver from network + visibility signals.
 * Auth is intentionally not inferred from browser primitives.
 */
export function createBrowserConnectionDriver(
  input: CreateBrowserConnectionDriverInput = {},
): ConnectionDriver {
  const win = input.window ?? window;

  function deriveState(): ConnectionState {
    if (!win.navigator.onLine) {
      return "offline";
    }

    if (win.document.hidden) {
      return "paused";
    }

    return "connected";
  }

  return {
    subscribe(listener: (state: ConnectionState) => void): () => void {
      let current = deriveState();
      listener(current);

      const emitIfChanged = () => {
        const next = deriveState();
        if (next === current) {
          return;
        }

        current = next;
        listener(next);
      };

      win.addEventListener("online", emitIfChanged);
      win.addEventListener("offline", emitIfChanged);
      win.document.addEventListener("visibilitychange", emitIfChanged);

      return () => {
        win.removeEventListener("online", emitIfChanged);
        win.removeEventListener("offline", emitIfChanged);
        win.document.removeEventListener("visibilitychange", emitIfChanged);
      };
    },
  };
}
