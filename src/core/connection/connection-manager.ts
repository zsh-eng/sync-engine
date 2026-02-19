import type { ConnectionDriver, ConnectionManager, ConnectionState } from "../types";

export interface CreateConnectionManagerInput {
  driver: ConnectionDriver;
  initialState?: ConnectionState;
}

export function createConnectionManager(input: CreateConnectionManagerInput): ConnectionManager {
  const listeners = new Set<(state: ConnectionState) => void>();
  let state = input.initialState ?? "offline";

  input.driver.subscribe((nextState) => {
    if (state === nextState) {
      return;
    }

    state = nextState;

    for (const listener of listeners) {
      listener(state);
    }
  });

  return {
    getState: () => state,
    subscribe(listener: (nextState: ConnectionState) => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
