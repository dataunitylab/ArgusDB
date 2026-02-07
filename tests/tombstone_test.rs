use argusdb::db::DB;
use argusdb::serde_to_jsonb;
use serde_json::json;
use tempfile::tempdir;

const MEMTABLE_THRESHOLD: usize = 5;
const JSTABLE_THRESHOLD: u64 = 5;
const INDEX_THRESHOLD: u64 = 1024;

#[test]
fn test_tombstone_and_shadowing() {
    let dir = tempdir().unwrap();
    let mut db = DB::new(
        dir.path().to_str().unwrap(),
        MEMTABLE_THRESHOLD,
        JSTABLE_THRESHOLD,
        INDEX_THRESHOLD,
        Some(1024 * 1024),
    );
    db.create_collection("test").unwrap();

    // 1. Insert doc1 and Flush to disk
    let doc1 = serde_to_jsonb(json!({ "val": 10 }));
    let id = db.insert("test", doc1.clone()).unwrap();

    // Trigger flush
    for i in 0..MEMTABLE_THRESHOLD {
        db.insert("test", serde_to_jsonb(json!({ "fill": i })))
            .unwrap();
    }
    db.wait_for_flush("test").unwrap();

    // Verify on disk
    assert_eq!(db.get("test", &id).unwrap(), Some(doc1.clone()));

    // 2. Update doc1 (Shadows disk)
    let doc1_v2 = serde_to_jsonb(json!({ "val": 20 }));
    db.update("test", &id, doc1_v2.clone()).unwrap();

    // Verify shadow (should be v2)
    assert_eq!(db.get("test", &id).unwrap(), Some(doc1_v2.clone()));

    // 3. Delete doc1 (Tombstone in MemTable)
    db.delete("test", &id).unwrap();

    // Verify tombstone (should be None)
    assert_eq!(db.get("test", &id).unwrap(), None);

    // 4. Flush the delete to disk
    for i in 0..MEMTABLE_THRESHOLD {
        db.insert("test", serde_to_jsonb(json!({ "fill_2": i })))
            .unwrap();
    }
    db.wait_for_flush("test").unwrap();

    // 5. Verify tombstone on disk
    // The previous JSTable has the record. The new JSTable has the tombstone (implicitly or explicitly depending on compaction).
    // ArgusDB implementation:
    // read_jstable reads all tables.
    // get checks newer tables first.
    // if newer table has tombstone (Null), it returns None immediately.
    assert_eq!(db.get("test", &id).unwrap(), None);

    // 6. Verify scan doesn't return it
    let results: Vec<_> = db
        .scan("test", None, None)
        .unwrap()
        .filter(|r| r.id() == id)
        .collect();
    assert!(results.is_empty());
}
