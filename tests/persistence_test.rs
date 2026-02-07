use argusdb::db::DB;
use argusdb::serde_to_jsonb;
use serde_json::json;
use tempfile::tempdir;

const MEMTABLE_THRESHOLD: usize = 5;
const JSTABLE_THRESHOLD: u64 = 5;
const INDEX_THRESHOLD: u64 = 1024;

#[test]
fn test_multiple_jstable_recovery() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap();

    {
        let mut db = DB::new(
            db_path,
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            INDEX_THRESHOLD,
            Some(1024 * 1024),
        );
        db.create_collection("test").unwrap();

        // Trigger multiple flushes to create multiple JSTables
        // We want at least 2 JSTables.
        // MEMTABLE_THRESHOLD is 5.
        // Inserting 5 items -> Memtable full.
        // Inserting 6th item -> Flush triggered (creates jstable-0), new item in Memtable.
        // Inserting 5 more items -> Memtable full again (6 total).
        // Inserting 11th item -> Flush triggered (creates jstable-1).

        for i in 0..15 {
            db.insert("test", serde_to_jsonb(json!({ "val": i })))
                .unwrap();
            // Small sleep to ensure timestamps might differ, though not strictly necessary for this test
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        // Wait for flushes to complete
        db.wait_for_flush("test").unwrap();
    } // db dropped here

    // Re-open DB
    {
        let db = DB::new(
            db_path,
            MEMTABLE_THRESHOLD,
            JSTABLE_THRESHOLD,
            INDEX_THRESHOLD,
            Some(1024 * 1024),
        );

        // We expect at least 2 JSTables (jstable-0, jstable-1) if 15 items were inserted
        // 0-4 (flush), 5-9 (flush), 10-14 (in memtable or flushed depending on exact logic)
        // Let's check internal state if possible, or just verify data.
        // Since we can't easily access private fields like jstable_count without creating a test in src/lib,
        // we will verify data presence which implicitly proves they were loaded.

        let results: Vec<_> = db.scan("test", None, None).unwrap().collect();
        assert_eq!(results.len(), 15, "Should recover all 15 documents");
    }
}
