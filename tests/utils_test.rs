use argusdb::db::DB;
use argusdb::serde_to_jsonb;
use serde_json::json;
use tempfile::tempdir;

const MEMTABLE_THRESHOLD: usize = 5;
const JSTABLE_THRESHOLD: u64 = 5;
const INDEX_THRESHOLD: u64 = 1024;

#[test]
fn test_create_collection_sanitization() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().to_str().unwrap();

    let mut db = DB::new(
        db_path,
        MEMTABLE_THRESHOLD,
        JSTABLE_THRESHOLD,
        INDEX_THRESHOLD,
        Some(1024 * 1024),
    );

    let problematic_name = "user/data";
    // '/' is 0x2f
    let expected_dir_name = "user_2fdata";

    db.create_collection(problematic_name).unwrap();

    // Verify directory exists on disk with sanitized name
    let expected_path = dir.path().join(expected_dir_name);
    assert!(
        expected_path.exists(),
        "Sanitized directory should exist: {:?}",
        expected_path
    );

    // Verify we can interact with it using the original name
    let doc = serde_to_jsonb(json!({ "foo": "bar" }));
    db.insert(problematic_name, doc.clone()).unwrap();

    let results: Vec<_> = db.scan(problematic_name, None, None).unwrap().collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get_value(), doc);

    // Verify verifying the collection exists
    let collections = db.show_collections();
    assert!(collections.contains(&problematic_name.to_string()));
}
