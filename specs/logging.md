# Log file format

The log file is a sequence of JSON objects, one per line. Each object represents a single operation that modifies the database state.

## Log entry format

Each log entry is a JSON object with the following fields:

- `ts`: An ISO 8601 timestamp of when the operation occurred.
- `op`: The type of operation. Can be one of "insert", "update", or "delete".
- `doc`: The document for "insert" and "update" operations.
- `id`: The document ID for "update" and "delete" operations.

### Insert operation

```json
{
  "ts": "2026-01-10T12:00:00.000Z",
  "op": "insert",
  "doc": { "a": 1 }
}
```

### Update operation

```json
{
  "ts": "2026-01-10T12:00:01.000Z",
  "op": "update",
  "id": "01H4J3J4J3J4J3J4J3J4J3J4J3",
  "doc": { "b": "hello" }
}
```

### Delete operation

```json
{
  "ts": "2026-01-10T12:00:02.000Z",
  "op": "delete",
  "id": "01H4J3J4J3J4J3J4J3J4J3J4J3"
}
```

# Log rotation

When the log file reaches a certain size, a new log file is created. The old log file can be archived or deleted.
The exact mechanism for rotation and archiving is to be determined.
