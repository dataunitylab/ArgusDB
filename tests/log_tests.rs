use argusdb::log::{Log, Logger, Operation};
use argusdb::serde_to_jsonb;
use serde_json::json;
use tempfile::NamedTempFile;

#[test]
fn test_log_auto_rotation() {
    let log_file = NamedTempFile::new().unwrap();
    // Small threshold to force rotation
    let threshold = 100;
    let mut logger = Logger::new(log_file.path(), threshold).unwrap();

    // Create a doc that is large enough or write many times
    let doc = serde_to_jsonb(json!({"a": "x".repeat(50)}));
    let op = Operation::Insert {
        id: "id".to_string(),
        doc,
    };

    // 1. Size 0. Write ~80. Size ~80.
    logger.log(op.clone()).unwrap();
    // 2. Size ~80. Not > 100. Write ~80. Size ~160.
    logger.log(op.clone()).unwrap();
    // 3. Size ~160. > 100. Rotate! Write ~80 to new file. Size ~80.
    logger.log(op.clone()).unwrap();

    let rotated_path = log_file.path().with_extension("log.1");
    assert!(rotated_path.exists());
    let rotated_content = std::fs::read_to_string(rotated_path).unwrap();
    assert!(!rotated_content.is_empty());
}
