use crate::jstable::JSTable;
use crate::schema::{Schema, infer_schema};
use serde_json::Value;
use std::collections::BTreeMap;

pub struct MemTable {
    pub documents: BTreeMap<String, Value>,
    schema: Schema,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            documents: BTreeMap::new(),
            schema: Schema {
                types: vec![],
                properties: None,
                items: None,
            },
        }
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn flush(&self, path: &str) -> std::io::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let jstable = JSTable::new(timestamp, self.schema.clone(), self.documents.clone());
        jstable.write(path)
    }

    pub fn insert(&mut self, id: String, doc: Value) {
        let doc_schema = infer_schema(&doc);
        self.schema.merge(doc_schema);
        self.documents.insert(id, doc);
    }

    pub fn update(&mut self, id: &str, doc: Value) {
        let doc_schema = infer_schema(&doc);
        self.schema.merge(doc_schema);
        self.documents.insert(id.to_string(), doc);
    }

    pub fn delete(&mut self, id: &str) {
        self.documents.insert(id.to_string(), Value::Null);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaType;
    use serde_json::json;

    #[test]
    fn test_memtable_insert() {
        let mut memtable = MemTable::new();
        memtable.insert("test-id".to_string(), json!({"a": 1}));
        memtable.insert("test-id-2".to_string(), json!({"b": "hello"}));

        assert_eq!(memtable.len(), 2);

        let schema = memtable.schema;
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }

    #[test]
    fn test_memtable_update() {
        let mut memtable = MemTable::new();
        memtable.insert("test-id".to_string(), json!({"a": 1}));
        memtable.update("test-id", json!({"b": "hello"}));

        assert_eq!(memtable.len(), 1);
        let doc = memtable.documents.get("test-id").unwrap();
        assert_eq!(*doc, json!({"b": "hello"}));

        let schema = memtable.schema;
        assert_eq!(schema.types, vec![SchemaType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(props.get("a").unwrap().types, vec![SchemaType::Integer]);
        assert_eq!(props.get("b").unwrap().types, vec![SchemaType::String]);
    }
}
