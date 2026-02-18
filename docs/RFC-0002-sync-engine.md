# RFC-0002: Architecture for a Simplified Sync Engine `microsync`

## 1. Summary

This RFC proposes a much simplified sync architecture with a narrow scope and an opinionated default experience. It differs
from the previous RFC in that the interfaces are much simpler, resulting in lesser code.

## 2. Problem

We want to handle the following problems:

- Multiple devices per user
- Offline edits and reconnect
- Conflict handling
- Serverless deployment constraints

Existing systems are for broader problem spaces, typically handling
much larger volumes of data.
This RFC focuses on a narrower product surface of personal / user-owned data.

## 3. Proposed Architecture

### "Bag of Rows"

Data is a stored as a "bag or rows" with the following fields for each row:

- `userId`: (optional - for nodes that handle multiple users)
- `namespace`: Then namespace for the app
- `id`: Unique identifier for the row
- `parent_id`: Identifier of the parent row, if any
- `data`: JSON-encoded data for the row
- `tombstone`: Boolean indicating whether the row has been deleted
- `txId`: optional transaction identifier
- `committed_timestamp_ms`: Timestamp in milliseconds when the row was committed in the current storage
- `hlc_timestamp_ms`: Hybrid Logical Clock timestamp in milliseconds
- `hlc_counter`: Hybrid Logical Clock counter
- `hlc_device_id`: Identifier of the device that created the row, for deterministic tie-breaking of HLC

Note: `parent_id` is used to represent one-to-many relationships.

Why "bag of rows"?

It allows us to easily sync data with arbitrary schemas, and allow multiple apps to share the same "sync server / database".
It is essentially a "schema-on-read" approach, similar to a NoSQL database.

### Storage

A `Storage` interface is design to execute the kinds of storage operations that our app ought to handle, which include:

- Retrieving a single item by its ID (`get`)
- Retrieving all items in a collection (`getAll`)
- Retrieving all items in a collection with a specific parent (`getAllWithParent`)
- Storing a new item (`put`)
- Deleting a single item by its ID (`delete`)
- Deleting all items in a collection with a specific parent (`deleteAllWithParent`)
- Deleting pending operation log entries (`deletePendingOperations`)

In addition, the `Storage` interface should provide a KV store that allows us to persist key-value pairs (`putKV`, `getKV`, `deleteKV`).
This KV store will be used to store metadata such as our sync cursors.

### Storage Adapter

Storage adapters are responsible for executing the Storage operations.
Operations are "pushed down" to the storage adapter level, such that each adapter can efficiently execute the operations.

For instance, the `SQLiteStorageAdapter` can use `ON CONFLICT DO UPDATE ... WHERE` to efficiently execute the LWW strategy on a large number of rows. On the other hand, the `DexieStorageAdapter` will use `bulkGet` to read from memory, execute the LWW in memory and `bulkPut` in a transaction to write to memory.

The storage adapters can also decide how "deletes" are handled, and what datatypes the tombstones are stored as.

In addition, the storage adapters will have a separate table for handling "pending" entries that have not yet been synced to the server.

The storage adapter will also handle the creation of the row and operation log tables, as well as any necessary indexes.

### Pending Operations

When local writes occur, they are written to a pending "operation log" table.
The operation log table must be sequenced, so that we can easily send the pending operations to the server batched, and in the correct order.
The simplest way of handling this is to use an auto-incrementing primary key in the oplog table.

When an operation is synced with the server, it can be removed from the pending operation log table.

### Hybrid Logical Clock (HLC)

We employ hybrid logic clocks to handle the last-write-wins conflict resolution.
We use HLCs instead of vector clocks as the total number of devices are not known upfront.

### Commit Timestamps

Apart from the HLC, there is also the *timestamp* of the commit.
This allows us to know when the operation was committed.

It is useful for the "server" side as the a combination of `timestamp, collection, id` (for a specific user) can be used to create cursor that allows the client to fetch the latest changes.
This assumes that our server itself doesn't experience clock skew (at least, doesn't go backward when there are writes happening).

### Client vs Server

Fundamentally, there is no difference between "client" and "server".
Because we are storing the commit timestamps, any "client" can be converted into a server that serves requests from others, ordered by its local commit timestamp.

Changing Servers.

To change servers, you just go to the new server with no pull cursor, and traverse through the operations on that new server (this is already necessary to do if it's a brand new client that just logged in, so not much difference in that regard).

## ConnectionManager

Connection manager is a finite state machine that handles states like OFFLINE, CONNECTED, NEEDS AUTH, etc.

Connection manager has a driver / certain bindings - e.g. browser bindings that hook into online/offline and visibility events or react native bindings that do something similar.

## Transport

Transport is a layer that handles communication between the sync engine and the server.

Has functions like
`push, pull, onServerEvent`, etc.

One example of transport is `HttpTransport`. In the factory we can define how the auth is handled, either by bearer auth token or session cookie `includeCredentials`.

## Sync Loop

The sync loop is driven by the `SyncEngine`, which is subscribes to events from the `ConnectionManager` to decide when to start / stop its pinging loop.

It retrieves the pending rows from the `Storage` and then calls `push`.
It maintains the sync cursor from the server by reading / writing from storage and calls `pull` to retrieve the latest changes from the server.

## 4. Issues

### Clock Skew

We accept a slight degree of clock skew between devices.
Clock skew is handled by the hybrid logical clock.
However, we assume that there are not cases of large clock skew (which might cause our operations to converge on something that we don't desire to, or data loss if the clock goes backward).

As for "server" commit timestamps, they are mainly used such that clients can quickly query the server.
It is still possible for us to return all the rows that the server is storing, and the LWW strategy with HLC ensures that the clients will still converge on the correct state.
We accept that the client might miss some data if the server experiences a clock skew, but this is an acceptable trade-off and unlikely to occur as our use case is personal apps, which have low write frequency and low chance of concurrent writes.

## 5. Questions

1. What should we name the storage and adapters - are the names appropriate?
2. Should we name the pending table "operation log" or "pending sync operations" or something else?
3. Should our "bag of rows" also have the schema version?
4. Should indices be by user, namespace, collection or something else? and also user, namespace, committed_timestamp
5. Should the server filter the sync reply to exclude the "deviceId" of the one sending?
