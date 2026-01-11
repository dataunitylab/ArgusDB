use crate::log::{Logger, Operation};
use crate::schema::{infer_schema, Schema};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub struct MemTable {
    documents: BTreeMap<String, Value>,
    schema: Schema,
    logger: Logger,
}

impl MemTable {
    pub fn new(log_path: &str) -> Self {
        let logger = Logger::new(log_path).unwrap();
        let mut memtable = MemTable {
            documents: BTreeMap::new(),
            schema: Schema {
                types: vec![],
                properties: None,
                items: None,
            },
            logger,
        };
        memtable.recover(log_path);
        memtable
    }

        fn recover(&mut self, log_path: &str) {

            let log_content = std::fs::read_to_string(log_path).unwrap_or_default();

            for line in log_content.lines() {

                if line.is_empty() {

                    continue;

                }

                let entry: crate::log::LogEntry = serde_json::from_str(line).unwrap();

                match entry.op {

                    Operation::Insert { id, doc } => {

                        self.insert_with_id(&id, doc);

                    }

                    Operation::Update { id, doc } => {

                        self._update(&id, doc);

                    }

                    Operation::Delete { id } => {

                        self._delete(&id);

                    }

                }

            }

        }

    

        fn insert_with_id(&mut self, id: &str, doc: Value) {

            let doc_schema = infer_schema(&doc);

            self.schema.merge(doc_schema);

            self.documents.insert(id.to_string(), doc);

        }

    

        pub fn insert(&mut self, doc: Value) -> String {

            let doc_schema = infer_schema(&doc);

            self.schema.merge(doc_schema.clone());

            let id = Uuid::now_v7().to_string();

            self.documents.insert(id.clone(), doc.clone());

            self.logger

                .log(Operation::Insert {

                    id: id.clone(),

                    doc,

                })

                .expect("Failed to log insert");

            id

        }

    

        fn _delete(&mut self, id: &str) {

            self.documents.remove(id);

        }

    

        pub fn delete(&mut self, id: &str) {

            self._delete(id);

            self.logger

                .log(Operation::Delete { id: id.to_string() })

                .expect("Failed to log delete");

        }

    

        fn _update(&mut self, id: &str, doc: Value) {

            let doc_schema = infer_schema(&doc);

            self.schema.merge(doc_schema);

            self.documents.insert(id.to_string(), doc);

        }

    

        pub fn update(&mut self, id: &str, doc: Value) {

            self._update(id, doc.clone());

            self.logger

                .log(Operation::Update {

                    id: id.to_string(),

                    doc,

                })

                .expect("Failed to log update");

        }

    }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaType;
    use serde_json::json;
    use tempfile::NamedTempFile;

    fn create_test_memtable() -> (NamedTempFile, MemTable) {
        let log_file = NamedTempFile::new().unwrap();
        let memtable = MemTable::new(log_file.path().to_str().unwrap());
        (log_file, memtable)
    }

    #[test]
    fn test_memtable_insert() {
        let (_log_file, mut memtable) = create_test_memtable();
        memtable.insert(json!({"a": 1}));
        memtable.insert(json!({"b": "hello"}));

        assert_eq!(memtable.documents.len(), 2);

        let schema = memtable.schema;
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }

    #[test]
    fn test_memtable_delete() {
        let (_log_file, mut memtable) = create_test_memtable();
        let id = memtable.insert(json!({"a": 1}));
        assert_eq!(memtable.documents.len(), 1);
        memtable.delete(&id);
        assert_eq!(memtable.documents.len(), 0);
    }

    #[test]
    fn test_memtable_update() {
        let (_log_file, mut memtable) = create_test_memtable();
        let id = memtable.insert(json!({"a": 1}));
        memtable.update(&id, json!({"b": "hello"}));

        assert_eq!(memtable.documents.len(), 1);
        let doc = memtable.documents.get(&id).unwrap();
        assert_eq!(*doc, json!({"b": "hello"}));

        let schema = memtable.schema;
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }

    #[test]
    fn test_log_content() {
        let (log_file, mut memtable) = create_test_memtable();
        let doc1 = json!({"a": 1});
        let id1 = memtable.insert(doc1.clone());

        let doc2 = json!({"b": "hello"});
        memtable.update(&id1, doc2.clone());

        memtable.delete(&id1);

        let log_content = std::fs::read_to_string(log_file.path()).unwrap();
        let mut lines = log_content.lines();

        let entry1: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry1.op {
            Operation::Insert { id, doc } => {
                assert_eq!(id, id1);
                assert_eq!(doc, doc1);
            }
            _ => panic!("Expected insert operation"),
        }

        let entry2: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry2.op {
            Operation::Update { id, doc } => {
                assert_eq!(id, id1);
                assert_eq!(doc, doc2);
            }
            _ => panic!("Expected update operation"),
        }

        let entry3: crate::log::LogEntry = serde_json::from_str(lines.next().unwrap()).unwrap();
        match entry3.op {
            Operation::Delete { id } => assert_eq!(id, id1),
            _ => panic!("Expected delete operation"),
        }
    }

    #[test]
    fn test_memtable_recover() {
        let (log_file, mut memtable) = create_test_memtable();
        let doc1 = json!({"a": 1});
        let id1 = memtable.insert(doc1.clone());

        let doc2 = json!({"b": "hello"});
        let id2 = memtable.insert(doc2.clone());

        memtable.delete(&id1);

        let memtable2 = MemTable::new(log_file.path().to_str().unwrap());
        assert_eq!(memtable2.documents.len(), 1);
        assert_eq!(*memtable2.documents.get(&id2).unwrap(), doc2);
    }
}
