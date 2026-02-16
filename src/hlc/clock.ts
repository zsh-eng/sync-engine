export type HybridLogicalClock = string & {
  readonly __brand: "HybridLogicalClock";
};

export interface ParsedClock {
  wallMs: number;
  counter: number;
  nodeId: string;
}

export interface NextClockInput {
  lastClock?: HybridLogicalClock;
  nodeId: string;
  nowMs?: number;
}

export interface NextClockFromRemoteInput {
  lastLocalClock?: HybridLogicalClock;
  remoteClock: HybridLogicalClock;
  nodeId: string;
  nowMs?: number;
}

const CLOCK_PARTS = 3;

function assertFiniteInteger(value: number, field: string): void {
  if (!Number.isFinite(value) || !Number.isInteger(value) || value < 0) {
    throw new Error(`Invalid ${field}: expected a non-negative integer, received ${value}`);
  }
}

export function formatClock(parts: ParsedClock): HybridLogicalClock {
  assertFiniteInteger(parts.wallMs, "wallMs");
  assertFiniteInteger(parts.counter, "counter");

  if (parts.nodeId.length === 0) {
    throw new Error("Invalid nodeId: cannot be empty");
  }

  return `${parts.wallMs}-${parts.counter}-${parts.nodeId}` as HybridLogicalClock;
}

export function parseClock(clock: HybridLogicalClock): ParsedClock {
  const parts = clock.split("-");

  if (parts.length !== CLOCK_PARTS) {
    throw new Error(`Invalid HLC format: ${clock}`);
  }

  const [wallMsRaw, counterRaw, nodeId] = parts;
  const wallMs = Number(wallMsRaw);
  const counter = Number(counterRaw);

  assertFiniteInteger(wallMs, "wallMs");
  assertFiniteInteger(counter, "counter");

  if (nodeId.length === 0) {
    throw new Error("Invalid nodeId: cannot be empty");
  }

  return {wallMs, counter, nodeId};
}

export function compareClocks(a: HybridLogicalClock, b: HybridLogicalClock): -1 | 0 | 1 {
  const pa = parseClock(a);
  const pb = parseClock(b);

  if (pa.wallMs !== pb.wallMs) {
    return pa.wallMs < pb.wallMs ? -1 : 1;
  }

  if (pa.counter !== pb.counter) {
    return pa.counter < pb.counter ? -1 : 1;
  }

  if (pa.nodeId === pb.nodeId) {
    return 0;
  }

  return pa.nodeId < pb.nodeId ? -1 : 1;
}

export function nextClock(input: NextClockInput): HybridLogicalClock {
  const nowMs = input.nowMs ?? Date.now();
  assertFiniteInteger(nowMs, "nowMs");

  const last = input.lastClock ? parseClock(input.lastClock) : undefined;

  if (!last) {
    return formatClock({wallMs: nowMs, counter: 0, nodeId: input.nodeId});
  }

  if (nowMs > last.wallMs) {
    return formatClock({wallMs: nowMs, counter: 0, nodeId: input.nodeId});
  }

  return formatClock({wallMs: last.wallMs, counter: last.counter + 1, nodeId: input.nodeId});
}

export function nextClockFromRemote(input: NextClockFromRemoteInput): HybridLogicalClock {
  const nowMs = input.nowMs ?? Date.now();
  assertFiniteInteger(nowMs, "nowMs");

  const local = input.lastLocalClock ? parseClock(input.lastLocalClock) : undefined;
  const remote = parseClock(input.remoteClock);

  const localWall = local?.wallMs ?? Number.NEGATIVE_INFINITY;
  const localCounter = local?.counter ?? 0;

  const maxWallMs = Math.max(nowMs, localWall, remote.wallMs);

  let counter: number;

  if (maxWallMs === localWall && maxWallMs === remote.wallMs) {
    counter = Math.max(localCounter, remote.counter) + 1;
  } else if (maxWallMs === localWall) {
    counter = localCounter + 1;
  } else if (maxWallMs === remote.wallMs) {
    counter = remote.counter + 1;
  } else {
    counter = 0;
  }

  return formatClock({
    wallMs: maxWallMs,
    counter,
    nodeId: input.nodeId,
  });
}
