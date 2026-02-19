import { describe, expect, test } from "bun:test";

import {
  createHttpTransportAdapter,
  type CreateHttpTransportAdapterInput,
} from "./http-transport-adapter";

interface BookValue {
  title: string;
}

interface HighlightValue {
  note: string;
}

interface Collections {
  books: BookValue;
  highlights: HighlightValue;
}

type FetchLike = NonNullable<CreateHttpTransportAdapterInput<Collections>["fetch"]>;
type FetchInit = Parameters<FetchLike>[1];

interface MockFetchCall {
  input: string;
  init: FetchInit;
}

function jsonResponse(body: unknown, input: { status?: number; statusText?: string } = {}) {
  const status = input.status ?? 200;
  const statusText = input.statusText ?? "OK";

  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    async json() {
      return structuredClone(body);
    },
    async text() {
      return typeof body === "string" ? body : JSON.stringify(body);
    },
  };
}

function textErrorResponse(status: number, text: string, statusText = "Error") {
  return {
    ok: false,
    status,
    statusText,
    async json() {
      throw new Error("JSON body unavailable");
    },
    async text() {
      return text;
    },
  };
}

function makePutOperation() {
  return {
    sequence: 3,
    type: "put" as const,
    collectionId: "books" as const,
    id: "book-1",
    parentId: null,
    data: { title: "Dune" },
    txId: "tx_1",
    schemaVersion: 1,
    hlcTimestampMs: 1_000,
    hlcCounter: 0,
    hlcDeviceId: "deviceA",
  };
}

describe("createHttpTransportAdapter", () => {
  test("pull builds RFC query params and parses response", async () => {
    const calls: MockFetchCall[] = [];
    const fetch: FetchLike = async (input, init) => {
      calls.push({ input, init });
      return jsonResponse({
        changes: [
          {
            namespace: "books-app",
            collectionId: "books",
            id: "book-1",
            parentId: null,
            data: { title: "Dune" },
            tombstone: false,
            txId: "tx_remote",
            schemaVersion: 1,
            committedTimestampMs: 5_000,
            hlcTimestampMs: 4_000,
            hlcCounter: 2,
            hlcDeviceId: "deviceB",
          },
        ],
        nextCursor: {
          committedTimestampMs: 5_000,
          collectionId: "books",
          id: "book-1",
        },
        hasMore: false,
      });
    };

    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      namespace: "books-app",
      fetch,
      headers: {
        "x-client": "test-suite",
      },
    });

    const response = await transport.pull({
      limit: 50,
      collectionId: "books",
      parentId: "shelf-1",
      cursor: {
        committedTimestampMs: 4_000,
        collectionId: "books",
        id: "book-0",
      },
    });

    expect(response.hasMore).toBe(false);
    expect(response.changes).toHaveLength(1);
    expect(response.changes[0]).toMatchObject({
      collectionId: "books",
      id: "book-1",
      data: { title: "Dune" },
    });
    expect(response.nextCursor).toEqual({
      committedTimestampMs: 5_000,
      collectionId: "books",
      id: "book-1",
    });

    expect(calls).toHaveLength(1);
    const call = calls[0]!;
    const url = new URL(call.input);

    expect(url.pathname).toBe("/sync/pull");
    expect(url.searchParams.get("limit")).toBe("50");
    expect(url.searchParams.get("collectionId")).toBe("books");
    expect(url.searchParams.get("parentId")).toBe("shelf-1");
    expect(url.searchParams.get("namespace")).toBe("books-app");
    expect(url.searchParams.get("cursorCommittedTimestampMs")).toBe("4000");
    expect(url.searchParams.get("cursorCollectionId")).toBe("books");
    expect(url.searchParams.get("cursorId")).toBe("book-0");
    expect(call.init?.method).toBe("GET");
    expect(call.init?.credentials).toBe("include");
    expect(call.init?.headers).toMatchObject({
      "x-client": "test-suite",
    });
  });

  test("push sends JSON body and returns acknowledged sequence", async () => {
    const calls: MockFetchCall[] = [];
    const fetch: FetchLike = async (input, init) => {
      calls.push({ input, init });
      return jsonResponse({ acknowledgedThroughSequence: 3 });
    };

    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      namespace: "books-app",
      fetch,
      headers: {
        "x-client": "test-suite",
      },
    });

    const operation = makePutOperation();
    const response = await transport.push({
      operations: [operation],
    });

    expect(response).toEqual({ acknowledgedThroughSequence: 3 });
    expect(calls).toHaveLength(1);

    const call = calls[0]!;
    const url = new URL(call.input);

    expect(url.pathname).toBe("/sync/push");
    expect(call.init?.method).toBe("POST");
    expect(call.init?.credentials).toBe("include");
    expect(call.init?.headers).toMatchObject({
      "content-type": "application/json",
      "x-client": "test-suite",
    });
    expect(JSON.parse(call.init?.body ?? "{}")).toEqual({
      operations: [operation],
      namespace: "books-app",
    });
  });

  test("cookie auth uses include credentials by default", async () => {
    const calls: MockFetchCall[] = [];
    const fetch: FetchLike = async (input, init) => {
      calls.push({ input, init });
      return jsonResponse({ changes: [], hasMore: false });
    };

    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      auth: { type: "cookie" },
      fetch,
    });

    await transport.pull({ limit: 1 });
    expect(calls[0]?.init?.credentials).toBe("include");
  });

  test("bearer auth calls token callback per request and sets Authorization header", async () => {
    const calls: MockFetchCall[] = [];
    let tokenCalls = 0;

    const fetch: FetchLike = async (input, init) => {
      calls.push({ input, init });
      if (init?.method === "POST") {
        return jsonResponse({});
      }
      return jsonResponse({ changes: [], hasMore: false });
    };

    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      auth: {
        type: "bearer",
        token: () => `token-${++tokenCalls}`,
      },
      fetch,
    });

    await transport.pull({ limit: 5 });
    await transport.push({ operations: [] });

    expect(tokenCalls).toBe(2);
    expect(calls).toHaveLength(2);
    expect(calls[0]?.init?.headers?.authorization).toBe("Bearer token-1");
    expect(calls[1]?.init?.headers?.authorization).toBe("Bearer token-2");
    expect(calls[0]?.init?.credentials).toBeUndefined();
    expect(calls[1]?.init?.credentials).toBeUndefined();
  });

  test("401/403 emits needsAuth and throws", async () => {
    const events: string[] = [];
    const fetch: FetchLike = async () => textErrorResponse(401, "unauthorized", "Unauthorized");

    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      fetch,
    });

    transport.onEvent((event) => {
      events.push(event.type);
    });

    await expect(transport.pull({ limit: 1 })).rejects.toThrow(
      "HTTP GET /sync/pull failed with status 401 Unauthorized",
    );
    expect(events).toEqual(["needsAuth"]);
  });

  test("non-auth HTTP errors throw descriptive messages", async () => {
    const fetch: FetchLike = async () =>
      textErrorResponse(500, "boom failure while reading upstream", "Internal Server Error");
    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      fetch,
    });

    await expect(transport.pull({ limit: 1 })).rejects.toThrow(
      "HTTP GET /sync/pull failed with status 500 Internal Server Error: boom failure while reading upstream",
    );
  });

  test("invalid response payload is rejected", async () => {
    const fetch: FetchLike = async () => jsonResponse({ changes: [] });
    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      fetch,
    });

    await expect(transport.pull({ limit: 1 })).rejects.toThrow(
      'Invalid response payload: "hasMore" must be a boolean',
    );
  });

  test("onEvent unsubscribe prevents future notifications", async () => {
    const events: string[] = [];
    const fetch: FetchLike = async () => textErrorResponse(403, "forbidden", "Forbidden");
    const transport = createHttpTransportAdapter<Collections>({
      baseURL: "https://sync.example.com",
      fetch,
    });

    const unsubscribe = transport.onEvent((event) => {
      events.push(event.type);
    });
    unsubscribe();

    await expect(transport.pull({ limit: 1 })).rejects.toThrow(
      "HTTP GET /sync/pull failed with status 403 Forbidden",
    );
    expect(events).toEqual([]);
  });
});
