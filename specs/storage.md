ArgusDB uses log-structured merge trees, but instead of SSTables, we use JSTables.
A JSTable is like an SSTable, but stores semi-structured JSON data with an associated [JSON Schema](https://json-schema.org/).

# Disk format

Each JSTable is stored as two binary files using the [JSONB](https://github.com/databendlabs/jsonb) format: a summary file (`.summary`) and a data file (`.data`).

The files consist of a sequence of entries. Each entry is encoded as:
1.  **Length**: A 4-byte unsigned integer (little-endian) indicating the size of the following JSONB blob.
2.  **Data**: A binary blob encoded using the JSONB format.

## Structure

### Summary File

1.  **Header Entry**: The first entry in the file. It is a JSONB-encoded object containing:
    *   `timestamp`: The time the table was created (Unix timestamp in milliseconds).
    *   `schema`: The JSON Schema for the documents.
2.  **Filter Entry**: The second entry in the file. It is a [Binary Fuse8](https://github.com/ayazhafiz/xorf) filter of the record IDs in the table, serialized as a JSON byte vector.
3.  **Index Entry**: The third entry in the file. It is a sparse index mapping keys to byte offsets in the data file, serialized as a JSON byte vector. It is a list of `[key, offset]` pairs, created by adding an entry for the first key and then for every key that appears at least 1KB of data after the previous indexed key.

### Data File

1.  **Record Entries**: All entries. Each is a JSONB-encoded array `[id, document]`:
    *   `id`: String.
    *   `document`: The document object (or `null` for tombstone).

## Compression

Documents are stored using the JSONB binary format, which is generally more compact than text JSON.
A document can be `null` to indicate that it has been deleted.
Future versions might include schema-based compression.


# Compaction
