ArgusDB uses log-structured merge trees, but instead of SSTables, we use JSTables.
A JSTable is like an SSTable, but stores semi-structured JSON data with an associated [JSON Schema](https://json-schema.org/).

# Disk format

Each JSTable is stored in a single binary file using the [JSONB](https://github.com/databendlabs/jsonb) format.
The file consists of a sequence of entries. Each entry is encoded as:
1.  **Length**: A 4-byte unsigned integer (little-endian) indicating the size of the following JSONB blob.
2.  **Data**: A binary blob encoded using the JSONB format.

## Structure

1.  **Header Entry**: The first entry in the file. It is a JSONB-encoded object containing:
    *   `timestamp`: The time the table was created (Unix timestamp in milliseconds).
    *   `schema`: The JSON Schema for the documents.
2.  **Record Entries**: All subsequent entries. Each is a JSONB-encoded array `[id, document]`:
    *   `id`: String.
    *   `document`: The document object (or `null` for tombstone).

## Compression

Documents are stored using the JSONB binary format, which is generally more compact than text JSON.
A document can be `null` to indicate that it has been deleted.
Future versions might include schema-based compression.


# Compaction
