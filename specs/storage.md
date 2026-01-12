ArgusDB uses log-structured merge trees, but instead of SSTables, we use JSTables.
A JSTable is like an SSTable, but stores semi-structured JSON data with an associated [JSON Schema](https://json-schema.org/).

# Disk format

Each JSTable is stored in a single file.
This file must contain a JSON Schema as well as all the associated documents.

The format is as follows:
1.  The schema, serialized as JSON, followed by a newline.
2.  A sequence of records, one per line. Each record is a JSON array `[id, document]`.

Example:
```
{"type":"object","properties":{"a":{"type":["integer"]}}}
["01H4J3J4J3J4J3J4J3J4J3J4J3", {"a": 1}]
["01H4J3J4J3J4J3J4J3J4J3J4J4", {"a": 2}]
```

## Compression

Documents are stored as full JSON objects. This means that no compression is currently applied.
Future versions of the format might include schema-based compression. For example, if a field must be an integer, the type information need not be stored with each document. If a field can have multiple possible types, then the type information must be stored with each document.


# Compaction
