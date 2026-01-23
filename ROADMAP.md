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
- [x] Implement log rotation and deletion of old log files

# Stage 4 - Disk storage

This stage will allow the data stored in memory to be dumped to disk to allow storage of large data sets, resulting in a two-level LSM tree.

- [x] Define an on-disk format for JSTables in specs/storage.md including both the schema and the data
- [x] When documents are inserted past the in-memory threshold, write a new JSTable to disk and start a new in-memory structure

# Stage 5 - LSM compaction

This stage will implement the compaction algorithm for the LSM tree.

- [x] Implement compaction for JSTables by merging their schemas and updating the compressed documents

# Stage 6 - Internal query API

- [x] Define the operators and structures of the internal query algebra in specs/query-algebra.md
- [x] Implement selection of attributes from document collections
- [x] Implement filtering based on constraints on attributes

# Stage 7 - SQL Language

This stage will implement the query language of ArgusDB.

- [x] Refine the specification of the query language in specs/query-language.md
- [x] Implement a grammar for parsing the query language
- [x] Create query plans with the internal API from the parsed SQL
- [x] Implement unary numeric functions in SQL

# Stage 8 - Server protocol

The goal of this stage will be to connect the internal API for accessing ArgusDB to the Postgres wire protocol so clients can connect to the database.

- [x] Create a binary that starts a Postgres server
- [x] Receive SQL commands from the Postgres server and execute the queries against ArgusDB
- [x] Convert the results of the query into a format they can be passed along to the client
