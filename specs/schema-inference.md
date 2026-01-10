A key component of ArgusDB is the ability to infer a schema from a collection of JSON documents.
This allows ArgusDB to have the benefit of a schema for querying and storage optimization without the need to specify a schema upfront.
For each level of the log-structured merge tree, there is a single active schema.
First, documents in memory have an inferred schema that is updated as documents are added.
Each JSTables associated with a document collection has a single immutable schema.

# Single document discovery

For individual JSON documents, a simple schema can be inferred.

Example document:

    {"a": 1, "b": "foo"}

Example schema:

    {"type": "object", "properties": {"a": {"type": ["integer"]}, "b": {"type": ["string"]}}}

# Multi-document discovery

Discovery of a schema from multiple documents works by merging schemas.
The schema for the in-memory level of the ArgusDB log-structured merge tree is constantly updated as new documents are inserted.
A new schema is inferred for each new document
If one schema has a new field, it is added to the schema for the in-memory level.
Whenever a new type is possible for an existing field, that type is added to the array of valid types.
When performing LSM compaction, the schemas for each JSTable are merged according to the same process.

Example schema one:

    {"type": "object", "properties": {"a": {"type": ["integer"]}, "b": {"type": ["string"]}}}

Example schema two:

    {"type": "object", "properties": {"b": {"type": ["integer"]}, "c": {"type": ["string"]}}}

Example merged schema:

    {"type": "object", "properties": {"a": {"type": ["integer"]}, "b": {"type": ["string, integer"]}, "c": {"type": ["string"]}}}
