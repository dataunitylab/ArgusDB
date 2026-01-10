ArgusDB uses log-structured merge trees, but instead of SSTables, we use JSTables.
A JSTable is like an SSTable, but stores semi-structured JSON data with an associated [JSON Schema](https://json-schema.org/).

# Disk format

Each JSTable is stored in a single file.
This file must contain a JSON Schema as well as all the associated documents.
Documents are compressed according to the schema.
For example, if a field must be an integer, the type information need not be stored with each document.
If a field can have multiple possible types, then the type information must be stored with each document.

# Compaction
