use argusdb::parser::parse;
use argusdb::query::Statement;
use bumpalo::Bump;

#[test]
fn test_parse_create_collection() {
    let sql = "CREATE COLLECTION test";
    let arena = Bump::new();
    let stmt = parse(sql, &arena).unwrap();
    if let Statement::CreateCollection { collection } = stmt {
        assert_eq!(collection, "test");
    } else {
        panic!("Expected CreateCollection");
    }
}

#[test]
fn test_parse_drop_collection() {
    let sql = "DROP COLLECTION test";
    let arena = Bump::new();
    let stmt = parse(sql, &arena).unwrap();
    if let Statement::DropCollection { collection } = stmt {
        assert_eq!(collection, "test");
    } else {
        panic!("Expected DropCollection");
    }
}

#[test]
fn test_parse_show_collections() {
    let sql = "SHOW COLLECTIONS";
    let arena = Bump::new();
    let stmt = parse(sql, &arena).unwrap();
    if let Statement::ShowCollections = stmt {
    } else {
        panic!("Expected ShowCollections");
    }
}

#[test]
fn test_parse_invalid_sql() {
    let sql = "SELECT * FROM";
    let arena = Bump::new();
    let res = parse(sql, &arena);
    assert!(res.is_err());
}

#[test]
fn test_parse_insert_invalid_json() {
    let sql = "INSERT INTO test VALUES (`{invalid}`)";
    let arena = Bump::new();
    let res = parse(sql, &arena);
    assert!(res.is_err());
}

#[test]
fn test_parse_wildcard_unsupported() {
    let sql = "SELECT * FROM test";
    let arena = Bump::new();
    let res = parse(sql, &arena);
    assert!(res.is_err()); // "Wildcard * not supported yet"
}
