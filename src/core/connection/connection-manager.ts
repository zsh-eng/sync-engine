import type {
  AuthStatus,
  BackoffConfig,
  BackoffState,
  ConnectionState,
  ConnectionStateListener,
  NetworkStatus,
  SyncAbility,
  VisibilityStatus,
} from "./types";

const DEFAULT_BACKOFF: BackoffConfig = {
  baseMs: 1_000,
  maxMs: 60_000,
  multiplier: 2,
};

export interface ConnectionManager {
  /** Current snapshot of all connection state. */
  getState(): ConnectionState;

  /** Current backoff state. */
  getBackoff(): BackoffState;

  /** Derived sync ability from current state. */
  getSyncAbility(): SyncAbility;

  // --- External signal methods (idempotent) ---
  setOnline(): void;
  setOffline(): void;
  setAuthenticated(): void;
  setUnauthenticated(): void;
  setAuthExpired(): void;
  setVisible(): void;
  setHidden(): void;

  // --- Backoff tracking (does NOT notify connection state listeners) ---
  recordFailure(): void;
  recordSuccess(): void;

  /** Subscribe to connection state changes. Returns an unsubscribe function. */
  subscribe(listener: ConnectionStateListener): () => void;
}

export interface CreateConnectionManagerInput {
  initialNetwork?: NetworkStatus;
  initialAuth?: AuthStatus;
  initialVisibility?: VisibilityStatus;
  backoff?: Partial<BackoffConfig>;
  /** Injectable clock for testing. Defaults to Date.now. */
  now?: () => number;
}

/** Derive what the sync loop should do from current connection state. */
export function deriveSyncAbility(state: ConnectionState): SyncAbility {
  if (state.network === "offline") return "no-network";
  if (state.auth !== "authenticated") return "needs-auth";
  if (state.visibility === "hidden") return "paused";
  return "can-sync";
}

export function createConnectionManager(
  input: CreateConnectionManagerInput = {},
): ConnectionManager {
  const backoffConfig: BackoffConfig = { ...DEFAULT_BACKOFF, ...input.backoff };
  const now = input.now ?? Date.now;
  const listeners = new Set<ConnectionStateListener>();

  let state: ConnectionState = {
    network: input.initialNetwork ?? "online",
    auth: input.initialAuth ?? "unauthenticated",
    visibility: input.initialVisibility ?? "visible",
  };

  let backoff: BackoffState = {
    consecutiveFailures: 0,
    nextRetryAtMs: null,
  };

  function notify(previous: ConnectionState): void {
    for (const listener of listeners) {
      listener(state, previous);
    }
  }

  function setNetwork(value: NetworkStatus): void {
    if (state.network === value) return;
    const previous = state;
    state = { ...state, network: value };
    notify(previous);
  }

  function setAuth(value: AuthStatus): void {
    if (state.auth === value) return;
    const previous = state;
    state = { ...state, auth: value };
    notify(previous);
  }

  function setVisibility(value: VisibilityStatus): void {
    if (state.visibility === value) return;
    const previous = state;
    state = { ...state, visibility: value };
    notify(previous);
  }

  return {
    getState: () => state,
    getBackoff: () => backoff,
    getSyncAbility: () => deriveSyncAbility(state),

    setOnline: () => setNetwork("online"),
    setOffline: () => setNetwork("offline"),
    setAuthenticated: () => setAuth("authenticated"),
    setUnauthenticated: () => setAuth("unauthenticated"),
    setAuthExpired: () => setAuth("expired"),
    setVisible: () => setVisibility("visible"),
    setHidden: () => setVisibility("hidden"),

    recordFailure() {
      const failures = backoff.consecutiveFailures + 1;
      const delayMs = Math.min(
        backoffConfig.baseMs * Math.pow(backoffConfig.multiplier, failures - 1),
        backoffConfig.maxMs,
      );
      const jitter = delayMs * 0.25 * (Math.random() * 2 - 1);
      backoff = {
        consecutiveFailures: failures,
        nextRetryAtMs: now() + delayMs + jitter,
      };
    },

    recordSuccess() {
      backoff = {
        consecutiveFailures: 0,
        nextRetryAtMs: null,
      };
    },

    subscribe(listener: ConnectionStateListener): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
