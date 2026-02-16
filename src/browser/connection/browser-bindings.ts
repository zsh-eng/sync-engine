import type { ConnectionManager } from "../../core/connection/connection-manager";

export interface BrowserBindingsInput {
  manager: ConnectionManager;
  /** Injectable window for testing. Defaults to globalThis.window. */
  window?: Window;
}

/**
 * Wire browser events (online/offline, visibilitychange) to a ConnectionManager.
 * Auth is intentionally NOT wired here — it's app-specific.
 * Returns a cleanup function that removes all event listeners.
 */
export function bindBrowserEvents(input: BrowserBindingsInput): () => void {
  const win = input.window ?? window;
  const { manager } = input;

  const handleOnline = () => manager.setOnline();
  const handleOffline = () => manager.setOffline();
  const handleVisibility = () => {
    if (win.document.hidden) {
      manager.setHidden();
    } else {
      manager.setVisible();
    }
  };

  // Sync initial state
  if (!win.navigator.onLine) {
    manager.setOffline();
  }
  if (win.document.hidden) {
    manager.setHidden();
  }

  win.addEventListener("online", handleOnline);
  win.addEventListener("offline", handleOffline);
  win.document.addEventListener("visibilitychange", handleVisibility);

  return () => {
    win.removeEventListener("online", handleOnline);
    win.removeEventListener("offline", handleOffline);
    win.document.removeEventListener("visibilitychange", handleVisibility);
  };
}
