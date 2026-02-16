/** Whether the device can reach the network. */
export type NetworkStatus = "online" | "offline";

/** Whether the client has valid auth credentials for the sync server. */
export type AuthStatus = "authenticated" | "unauthenticated" | "expired";

/** Whether the browser tab/window is visible to the user. */
export type VisibilityStatus = "visible" | "hidden";

/** Snapshot of all connection-related signals. */
export interface ConnectionState {
  readonly network: NetworkStatus;
  readonly auth: AuthStatus;
  readonly visibility: VisibilityStatus;
}

/** What the sync loop should do given the current connection state. */
export type SyncAbility = "can-sync" | "no-network" | "needs-auth" | "paused";

/** Exponential backoff configuration. */
export interface BackoffConfig {
  /** Base delay in milliseconds. */
  baseMs: number;
  /** Maximum delay in milliseconds. */
  maxMs: number;
  /** Multiplier applied per consecutive failure. */
  multiplier: number;
}

/** Current backoff state tracked by the connection manager. */
export interface BackoffState {
  readonly consecutiveFailures: number;
  /** Timestamp (ms) after which the next retry is allowed. null = can try now. */
  readonly nextRetryAtMs: number | null;
}

/** Listener invoked when connection state changes. Receives current and previous state. */
export type ConnectionStateListener = (state: ConnectionState, previous: ConnectionState) => void;
