import type {
  AnyStoredRow,
  CollectionId,
  CollectionValueMap,
  PendingSequence,
  SyncCursor,
  TransportAdapter,
  TransportEvent,
  TransportPullRequest,
  TransportPullResponse,
  TransportPushRequest,
  TransportPushResponse,
} from "../types";

type HttpCredentials = "omit" | "same-origin" | "include";

interface HttpRequestInitLike {
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  credentials?: HttpCredentials;
}

interface HttpResponseLike {
  ok: boolean;
  status: number;
  statusText: string;
  json(): Promise<unknown>;
  text(): Promise<string>;
}

type HttpFetchLike = (input: string, init?: HttpRequestInitLike) => Promise<HttpResponseLike>;

type HttpBearerToken = string | null | undefined;

export type HttpTransportAdapterAuth =
  | {
      type: "cookie";
      credentials?: HttpCredentials;
    }
  | {
      type: "bearer";
      token: () => HttpBearerToken | Promise<HttpBearerToken>;
    };

export interface CreateHttpTransportAdapterInput<S extends CollectionValueMap> {
  baseURL: string;
  pullPath?: string;
  pushPath?: string;
  namespace?: string;
  auth?: HttpTransportAdapterAuth;
  headers?: Record<string, string>;
  fetch?: HttpFetchLike;
}

const DEFAULT_PULL_PATH = "/sync/pull";
const DEFAULT_PUSH_PATH = "/sync/push";
const DEFAULT_AUTH: HttpTransportAdapterAuth = {
  type: "cookie",
  credentials: "include",
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function expectString(value: unknown, path: string): string {
  if (typeof value !== "string") {
    throw new Error(`Invalid response payload: "${path}" must be a string`);
  }

  return value;
}

function expectBoolean(value: unknown, path: string): boolean {
  if (typeof value !== "boolean") {
    throw new Error(`Invalid response payload: "${path}" must be a boolean`);
  }

  return value;
}

function expectNumber(value: unknown, path: string): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Invalid response payload: "${path}" must be a finite number`);
  }

  return value;
}

function parseCursor(value: unknown, path: string): SyncCursor {
  if (!isRecord(value)) {
    throw new Error(`Invalid response payload: "${path}" must be an object`);
  }

  return {
    committedTimestampMs: expectNumber(value.committedTimestampMs, `${path}.committedTimestampMs`),
    collectionId: expectString(value.collectionId, `${path}.collectionId`),
    id: expectString(value.id, `${path}.id`),
  };
}

function parseStoredRow<S extends CollectionValueMap>(
  value: unknown,
  path: string,
): AnyStoredRow<S> {
  if (!isRecord(value)) {
    throw new Error(`Invalid response payload: "${path}" must be an object`);
  }

  if (!Object.hasOwn(value, "data")) {
    throw new Error(`Invalid response payload: "${path}.data" is required`);
  }

  const parentIdValue = value.parentId;
  if (parentIdValue !== null && typeof parentIdValue !== "string") {
    throw new Error(`Invalid response payload: "${path}.parentId" must be string | null`);
  }

  const txIdValue = value.txId;
  if (txIdValue !== undefined && typeof txIdValue !== "string") {
    throw new Error(`Invalid response payload: "${path}.txId" must be a string when provided`);
  }

  const schemaVersionValue = value.schemaVersion;
  if (
    schemaVersionValue !== undefined &&
    (typeof schemaVersionValue !== "number" || !Number.isFinite(schemaVersionValue))
  ) {
    throw new Error(
      `Invalid response payload: "${path}.schemaVersion" must be a finite number when provided`,
    );
  }

  return {
    namespace: expectString(value.namespace, `${path}.namespace`),
    collectionId: expectString(value.collectionId, `${path}.collectionId`) as CollectionId<S>,
    id: expectString(value.id, `${path}.id`),
    parentId: parentIdValue,
    data: value.data === null ? null : (structuredClone(value.data) as AnyStoredRow<S>["data"]),
    tombstone: expectBoolean(value.tombstone, `${path}.tombstone`),
    txId: txIdValue,
    schemaVersion: schemaVersionValue,
    committedTimestampMs: expectNumber(value.committedTimestampMs, `${path}.committedTimestampMs`),
    hlcTimestampMs: expectNumber(value.hlcTimestampMs, `${path}.hlcTimestampMs`),
    hlcCounter: expectNumber(value.hlcCounter, `${path}.hlcCounter`),
    hlcDeviceId: expectString(value.hlcDeviceId, `${path}.hlcDeviceId`),
  };
}

function parsePullResponse<S extends CollectionValueMap>(value: unknown): TransportPullResponse<S> {
  if (!isRecord(value)) {
    throw new Error("Invalid response payload: pull response must be an object");
  }

  if (!Array.isArray(value.changes)) {
    throw new Error('Invalid response payload: "changes" must be an array');
  }

  const changes = value.changes.map((row, index) => parseStoredRow<S>(row, `changes[${index}]`));
  const hasMore = expectBoolean(value.hasMore, "hasMore");
  const nextCursor =
    value.nextCursor === undefined ? undefined : parseCursor(value.nextCursor, "nextCursor");

  return {
    changes,
    nextCursor,
    hasMore,
  };
}

function parsePushResponse(value: unknown): TransportPushResponse {
  if (!isRecord(value)) {
    throw new Error("Invalid response payload: push response must be an object");
  }

  const acknowledgedThroughSequenceValue = value.acknowledgedThroughSequence;
  if (acknowledgedThroughSequenceValue === undefined) {
    return {};
  }

  return {
    acknowledgedThroughSequence: expectNumber(
      acknowledgedThroughSequenceValue,
      "acknowledgedThroughSequence",
    ) as PendingSequence,
  };
}

function resolveFetch(input?: HttpFetchLike): HttpFetchLike {
  if (input) {
    return input;
  }

  const candidate = (globalThis as { fetch?: unknown }).fetch;
  if (typeof candidate !== "function") {
    throw new Error("fetch is not available in this environment");
  }

  return candidate as HttpFetchLike;
}

function emitEvent<S extends CollectionValueMap>(
  listeners: ReadonlySet<(event: TransportEvent<S>) => void>,
  event: TransportEvent<S>,
): void {
  for (const listener of listeners) {
    listener(event);
  }
}

function trimErrorBody(body: string): string {
  const normalized = body.trim();
  if (normalized.length <= 240) {
    return normalized;
  }

  return `${normalized.slice(0, 240)}...`;
}

async function readErrorText(response: HttpResponseLike): Promise<string> {
  try {
    return trimErrorBody(await response.text());
  } catch {
    return "";
  }
}

function buildRequestURL(baseURL: string, path: string): URL {
  return new URL(path, baseURL);
}

export function createHttpTransportAdapter<S extends CollectionValueMap>(
  input: CreateHttpTransportAdapterInput<S>,
): TransportAdapter<S> {
  const listeners = new Set<(event: TransportEvent<S>) => void>();
  const fetchLike = resolveFetch(input.fetch);
  const pullPath = input.pullPath ?? DEFAULT_PULL_PATH;
  const pushPath = input.pushPath ?? DEFAULT_PUSH_PATH;
  const auth = input.auth ?? DEFAULT_AUTH;

  async function requestJSON(
    method: "GET" | "POST",
    url: URL,
    init: {
      headers?: Record<string, string>;
      body?: string;
    },
  ): Promise<unknown> {
    const headers: Record<string, string> = {
      ...(input.headers ?? {}),
      ...(init.headers ?? {}),
    };
    const requestInit: HttpRequestInitLike = {
      method,
      headers,
      ...(init.body !== undefined ? { body: init.body } : {}),
    };

    if (auth.type === "cookie") {
      requestInit.credentials = auth.credentials ?? "include";
    } else {
      const token = await auth.token();
      if (token) {
        requestInit.headers = {
          ...headers,
          authorization: `Bearer ${token}`,
        };
      }
    }

    const response = await fetchLike(url.toString(), requestInit);
    if (response.status === 401 || response.status === 403) {
      emitEvent(listeners, { type: "needsAuth" });
      const details = await readErrorText(response);
      throw new Error(
        `HTTP ${method} ${url.pathname} failed with status ${response.status} ${response.statusText}${
          details ? `: ${details}` : ""
        }`,
      );
    }

    if (!response.ok) {
      const details = await readErrorText(response);
      throw new Error(
        `HTTP ${method} ${url.pathname} failed with status ${response.status} ${response.statusText}${
          details ? `: ${details}` : ""
        }`,
      );
    }

    try {
      return await response.json();
    } catch {
      throw new Error(`HTTP ${method} ${url.pathname} returned invalid JSON`);
    }
  }

  return {
    async pull(request: TransportPullRequest<S>): Promise<TransportPullResponse<S>> {
      const url = buildRequestURL(input.baseURL, pullPath);
      url.searchParams.set("limit", String(request.limit));

      if (request.collectionId !== undefined) {
        url.searchParams.set("collectionId", request.collectionId);
      }
      if (request.parentId !== undefined) {
        url.searchParams.set("parentId", request.parentId);
      }
      if (input.namespace !== undefined) {
        url.searchParams.set("namespace", input.namespace);
      }
      if (request.cursor) {
        url.searchParams.set(
          "cursorCommittedTimestampMs",
          String(request.cursor.committedTimestampMs),
        );
        url.searchParams.set("cursorCollectionId", request.cursor.collectionId);
        url.searchParams.set("cursorId", request.cursor.id);
      }

      const payload = await requestJSON("GET", url, {});
      return parsePullResponse<S>(payload);
    },

    async push(request: TransportPushRequest<S>): Promise<TransportPushResponse> {
      const url = buildRequestURL(input.baseURL, pushPath);
      const body: {
        operations: Array<TransportPushRequest<S>["operations"][number]>;
        namespace?: string;
      } = {
        operations: request.operations.map((operation) => structuredClone(operation)),
      };

      if (input.namespace !== undefined) {
        body.namespace = input.namespace;
      }

      const payload = await requestJSON("POST", url, {
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
      return parsePushResponse(payload);
    },

    onEvent(listener: (event: TransportEvent<S>) => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
  };
}
