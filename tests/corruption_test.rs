use argusdb::jstable::{self, JSTable, StoredValue};
use argusdb::schema::{InstanceType, Schema, SchemaExt};
use argusdb::serde_to_jsonb;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use tempfile::tempdir;

fn create_valid_jstable(dir: &std::path::Path) -> String {
    let mut schema = Schema::new(InstanceType::Object);
    schema.properties = Some(BTreeMap::from([(
        "a".to_string(),
        Schema::new(InstanceType::Integer),
    )]));

    let mut documents = BTreeMap::new();
    documents.insert(
        "id1".to_string(),
        StoredValue::Static(serde_to_jsonb(json!({"a": 1}))),
    );

    let jstable = JSTable::new(12345, "test_col".to_string(), schema, documents);

    let file_path = dir.join("test_table");
    let path_str = file_path.to_str().unwrap();
    jstable.write(path_str, 1024).unwrap();
    path_str.to_string()
}

#[test]
fn test_read_corrupted_summary_header_len() {
    let dir = tempdir().unwrap();
    let path_str = create_valid_jstable(dir.path());
    let summary_path = format!("{}.summary", path_str);

    // Corrupt header length (make it huge)
    let mut file = OpenOptions::new().write(true).open(&summary_path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&u32::MAX.to_le_bytes()).unwrap();

    let res = jstable::read_jstable(&path_str);
    assert!(res.is_err());
    // Likely "failed to fill whole buffer" or OOM if it tries to allocate,
    // but IO Error is expected.
}

#[test]
fn test_read_corrupted_summary_truncated() {
    let dir = tempdir().unwrap();
    let path_str = create_valid_jstable(dir.path());
    let summary_path = format!("{}.summary", path_str);

    // Truncate file
    let file = File::create(&summary_path).unwrap(); // Truncates on open
    file.set_len(2).unwrap(); // valid u32 is 4 bytes

    let res = jstable::read_jstable(&path_str);
    assert!(res.is_err());
}

#[test]
fn test_read_corrupted_filter() {
    let dir = tempdir().unwrap();
    let path_str = create_valid_jstable(dir.path());

    // Valid read
    assert!(jstable::read_filter(&path_str).is_ok());

    let summary_path = format!("{}.summary", path_str);

    // Corrupt header length to be huge.
    // read_filter tries to skip this amount and should fail (EOF).
    let mut file = OpenOptions::new().write(true).open(&summary_path).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&u32::MAX.to_le_bytes()).unwrap();

    let res = jstable::read_filter(&path_str);
    assert!(res.is_err());
}

#[test]
fn test_read_corrupted_index() {
    let dir = tempdir().unwrap();
    let path_str = create_valid_jstable(dir.path());

    assert!(jstable::read_index(&path_str).is_ok());

    // Corrupt summary file heavily
    let summary_path = format!("{}.summary", path_str);
    let file = OpenOptions::new().write(true).open(&summary_path).unwrap();
    file.set_len(10).ok(); // make it very short

    let res = jstable::read_index(&path_str);
    assert!(res.is_err());
}

#[test]
fn test_read_jstable_missing_files() {
    let dir = tempdir().unwrap();
    let path_str = dir.path().join("missing").to_str().unwrap().to_string();

    assert!(jstable::read_jstable(&path_str).is_err());
}
