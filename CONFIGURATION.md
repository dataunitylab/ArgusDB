# Configuration parameters

All configuration parameters for ArgusDB are listed here.

*   `host`: The host to bind the server to (default: "127.0.0.1")
*   `port`: The port to bind the server to (default: 5432)
*   `memtable_threshold`: The maximum number of documents in memory before flushing (default: 10)
*   `jstable_threshold`: The maximum number of JSTables before compaction (default: 5)
*   `jstable_dir`: The directory to store JSTables (default: "argus_data")
*   `index_threshold`: The number of bytes of data between index entries (default: 1024)
