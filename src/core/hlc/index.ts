export type {
  HybridLogicalClock,
  NextClockFromRemoteInput,
  NextClockInput,
  ParsedClock,
} from "./clock";
export { compareClocks, formatClock, nextClock, nextClockFromRemote, parseClock } from "./clock";

export type {
  ClockService,
  ClockStorageAdapter,
  CreateClockServiceInput,
  MaybePromise,
} from "./service";
export { createClockService } from "./service";

