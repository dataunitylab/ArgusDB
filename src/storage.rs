use crate::schema::{infer_schema, Schema};
use serde_json::Value;
use std::collections::BTreeMap;
use uuid::Uuid;

pub struct MemTable {
    documents: BTreeMap<String, Value>,
    schema: Schema,
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

    pub fn insert(&mut self, doc: Value) -> String {
        let doc_schema = infer_schema(&doc);
        self.schema.merge(doc_schema);
        let id = Uuid::now_v7().to_string();
        self.documents.insert(id.clone(), doc);
        id
    }

    pub fn delete(&mut self, id: &str) {
        self.documents.remove(id);
    }

    pub fn update(&mut self, id: &str, doc: Value) {
        let doc_schema = infer_schema(&doc);
        self.schema.merge(doc_schema);
        self.documents.insert(id.to_string(), doc);
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
        let mut memtable = MemTable::new();
        let id = memtable.insert(json!({"a": 1}));
        assert_eq!(memtable.documents.len(), 1);
        memtable.delete(&id);
        assert_eq!(memtable.documents.len(), 0);
    }

    #[test]
    fn test_memtable_update() {
        let mut memtable = MemTable::new();
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
}
