# Stage 1 - Schema discovery

This stage will implement a basic version of JSON Schema discovery to be used in ArgusDB.

- [x] Infer a schema for an individual document
- [x] Merge schemas to allow inference of schemas for multiple documents

# Stage 2 - In-memory storage

This stage will implement a simple in-memory version of ArgusDB that will serve as the first layer of the LSM tree.

- [x] Accept documents for insertion into an in-memory data structure and infer a schema
- [x] Add documents to an in-memory data structure and update the inferred schema
- [x] Allow deletion of documents by ID
- [x] Allow updating of documents by ID

# Stage 3 - Logging

This stage will implement logging of all operations in ArgusDB to allow for fault tolerance.

- [x] Define a log file format in specs/logging.md
- [x] Add log entries whenever new documents are inserted
- [x] Implement log recovery to read the log on restart and recover from a crash
- [] Implement log rotation and deletion of old log files

# Stage 4 - Disk storage

This stage will allow the data stored in memory to be dumped to disk to allow storage of large data sets, resulting in a two-level LSM tree.

- [] Define an on-disk format for JSTables in specs/storage.md including both the schema and the data
- [] When documents are inserted past the in-memory threshold, write a new JSTable to disk and start a new in-memory structure

# Stage 5 - LSM compaction

This stage will implement the compaction algorithm for the LSM tree.

- [] Implement compaction for JSTables by merging their schemas and updating the compressed documents

# Stage 6 - Internal query API

- [] Define the operators and structures of the internal query algebra in specs/query-algebra.md
- [] Implement selection of attributes from document collections
- [] Implement filtering based on constraints on attributes

# Stage 7 - SQL Language

This stage will implement the query language of ArgusDB.

- [] Refine the specification of the query language in specs/query-language.md
- [] Implement a grammar for parsing the query language
- [] Create query plans with the internal API from the parsed SQL

# Stage 8 - Server protocol

The goal of this stage will be to connect the internal API for accessing ArgusDB to the Postgres wire protocol so clients can connect to the database.

- [] Create a binary that starts a Postgres server
- [] Receive SQL commands from the Postgres server and execute the queries against ArgusDB
- [] Convert the results of the query into a format they can be passed along to the client
