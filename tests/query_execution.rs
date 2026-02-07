use argusdb::db::DB;
use argusdb::expression::{BinaryOperator, Expression, LogicalOperator};
use argusdb::query::{LogicalPlan, execute_plan};
use argusdb::{Value, serde_to_jsonb};
use serde_json::json;
use tempfile::tempdir;

const MEMTABLE_THRESHOLD: usize = 1000;
const JSTABLE_THRESHOLD: u64 = 10;
const INDEX_THRESHOLD: u64 = 1024;

fn setup_db() -> (DB, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let mut db = DB::new(
        dir.path().to_str().unwrap(),
        MEMTABLE_THRESHOLD,
        JSTABLE_THRESHOLD,
        INDEX_THRESHOLD,
        None,
    );
    db.create_collection("test").unwrap();
    (db, dir)
}

#[test]
fn test_execute_scan() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 1}))).unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 2}))).unwrap();

    let plan = LogicalPlan::Scan {
        collection: "test".to_string(),
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_execute_filter_vectorized() {
    let (mut db, _dir) = setup_db();
    // Insert enough for batch size if needed, but 2 is fine for functionality
    db.insert("test", serde_to_jsonb(json!({"a": 1}))).unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 10}))).unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 20}))).unwrap();

    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        predicate: Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(5)))),
        },
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 2); // 10 and 20
}

#[test]
fn test_execute_filter_fallback() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 1, "b": 2})))
        .unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 10, "b": 20})))
        .unwrap();

    // Complex predicate (not simple binary field-literal)
    // a > 5 AND b > 10
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        predicate: Expression::Logical {
            left: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference(vec!["a"], "a")),
                op: BinaryOperator::Gt,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!(5)))),
            }),
            op: LogicalOperator::And,
            right: Box::new(Expression::Binary {
                left: Box::new(Expression::FieldReference(vec!["b"], "b")),
                op: BinaryOperator::Gt,
                right: Box::new(Expression::Literal(serde_to_jsonb(json!(10)))),
            }),
        },
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 1); // {"a": 10, "b": 20}
}

#[test]
fn test_execute_project() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 1, "b": 2})))
        .unwrap();

    let plan = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        projections: vec![Expression::FieldReference(vec!["a"], "a")],
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 1);
    let obj = results[0].as_object().unwrap();
    assert!(obj.contains_key("a"));
    assert!(!obj.contains_key("b"));
}

#[test]
fn test_execute_limit_offset() {
    let (mut db, _dir) = setup_db();
    for i in 0..10 {
        db.insert("test", serde_to_jsonb(json!({"a": i}))).unwrap();
    }

    let plan = LogicalPlan::Limit {
        input: Box::new(LogicalPlan::Offset {
            input: Box::new(LogicalPlan::Scan {
                collection: "test".to_string(),
            }),
            offset: 2,
        }),
        limit: 3,
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 3);
}

#[test]
fn test_execute_scan_batch_split() {
    let (mut db, _dir) = setup_db();
    let total_docs = 5000; // > 4096 default batch size
    for i in 0..total_docs {
        db.insert("test", serde_to_jsonb(json!({"a": i}))).unwrap();
    }

    let plan = LogicalPlan::Scan {
        collection: "test".to_string(),
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), total_docs);
}

#[test]
fn test_vectorized_filter_types() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 1.5})))
        .unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 2.5})))
        .unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 3.5})))
        .unwrap();

    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        predicate: Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(2.0)))),
        },
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 2); // 2.5, 3.5
}

#[test]
fn test_batch_limit_offset_crossing_batches() {
    let (mut db, _dir) = setup_db();
    let total_docs = 5000;
    for i in 0..total_docs {
        db.insert("test", serde_to_jsonb(json!({"a": i}))).unwrap();
    }

    // Offset 4090 (close to boundary 4096), Limit 20 (crosses boundary)
    let plan = LogicalPlan::Limit {
        input: Box::new(LogicalPlan::Offset {
            input: Box::new(LogicalPlan::Scan {
                collection: "test".to_string(),
            }),
            offset: 4090,
        }),
        limit: 20,
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 20);
    // Because insertion order is preserved in MemTable usually (sorted by ID or key?),
    // but here we just check count. ArgusDB might not guarantee order without ORDER BY unless implicit.
    // Assuming simple insert order or key order.
}

#[test]
fn test_batch_project_explicit() {
    let (mut db, _dir) = setup_db();
    // Use enough data to potentially trigger batching logic if we were careful,
    // but here just checking the operator works in vectorized mode.
    db.insert("test", serde_to_jsonb(json!({"a": 10, "b": 20})))
        .unwrap();

    let plan = LogicalPlan::Project {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        projections: vec![Expression::FieldReference(vec!["a"], "a")],
    };
    // Scan -> Project should be vectorizable

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 1);
    let obj = results[0].as_object().unwrap();
    assert!(obj.contains_key("a"));
    assert!(!obj.contains_key("b"));
}

#[test]
fn test_batch_filter_mixed_types() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 10}))).unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": "not a number"})))
        .unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 20}))).unwrap();

    // Filter a > 15. The string should be treated as non-match or handled gracefully in vectorized path.
    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        predicate: Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(15)))),
        },
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 1); // 20
    let val = results[0]
        .as_object()
        .unwrap()
        .get("a")
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(val, 20);
}

#[test]
fn test_vectorized_filter_disk() {
    let (mut db, _dir) = setup_db();
    db.insert("test", serde_to_jsonb(json!({"a": 1}))).unwrap();
    db.insert("test", serde_to_jsonb(json!({"a": 10}))).unwrap();

    // Flush to force disk storage (Lazy results)
    for i in 0..MEMTABLE_THRESHOLD {
        db.insert("test", serde_to_jsonb(json!({ "fill": i })))
            .unwrap();
    }
    db.wait_for_flush("test").unwrap();

    let plan = LogicalPlan::Filter {
        input: Box::new(LogicalPlan::Scan {
            collection: "test".to_string(),
        }),
        predicate: Expression::Binary {
            left: Box::new(Expression::FieldReference(vec!["a"], "a")),
            op: BinaryOperator::Gt,
            right: Box::new(Expression::Literal(serde_to_jsonb(json!(5)))),
        },
    };

    let iter = execute_plan(plan, &db).unwrap();
    let results: Vec<Value> = iter.map(|r| r.get_value()).collect();
    assert_eq!(results.len(), 1); // 10
}
