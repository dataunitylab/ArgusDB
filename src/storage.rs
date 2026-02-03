use crate::Value;
use crate::jstable::JSTable;
use crate::schema::{Schema, SchemaExt, infer_schema};
use std::collections::{BTreeMap, HashMap};

pub struct MemTable {
    pub documents: HashMap<String, Value>,
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
            documents: HashMap::new(),
            schema: Schema::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.documents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty()
    }

    pub fn flush(
        self,
        path: &str,
        collection: String,
        index_threshold: u64,
    ) -> std::io::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Sort documents by ID for JSTable
        let sorted_docs: BTreeMap<String, Value> = self.documents.into_iter().collect();

        let jstable = JSTable::new(timestamp, collection, self.schema, sorted_docs);
        jstable.write(path, index_threshold)
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
        use jsonb_schema::Value as JsonbValue;
        self.documents.insert(id.to_string(), JsonbValue::Null);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{InstanceType, SingleOrVec};
    use crate::serde_to_jsonb;
    use serde_json::json;

    fn get_types(schema: &Schema) -> Vec<InstanceType> {
        match &schema.instance_type {
            Some(SingleOrVec::Single(t)) => vec![t.clone()],
            Some(SingleOrVec::Vec(v)) => v.clone(),
            None => vec![],
        }
    }

    #[test]
    fn test_memtable_insert() {
        let mut memtable = MemTable::new();
        memtable.insert("test-id".to_string(), serde_to_jsonb(json!({"a": 1})));
        memtable.insert(
            "test-id-2".to_string(),
            serde_to_jsonb(json!({"b": "hello"})),
        );

        assert_eq!(memtable.len(), 2);

        let schema = memtable.schema;
        assert_eq!(get_types(&schema), vec![InstanceType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(
            get_types(props.get("a").unwrap()),
            vec![InstanceType::Integer]
        );
        assert_eq!(
            get_types(props.get("b").unwrap()),
            vec![InstanceType::String]
        );
    }

    #[test]
    fn test_memtable_update() {
        let mut memtable = MemTable::new();
        memtable.insert("test-id".to_string(), serde_to_jsonb(json!({"a": 1})));
        memtable.update("test-id", serde_to_jsonb(json!({"b": "hello"})));

        assert_eq!(memtable.len(), 1);
        let doc = memtable.documents.get("test-id").unwrap();
        // Comparing jsonb_schema::Value with another.
        assert_eq!(*doc, serde_to_jsonb(json!({"b": "hello"})));

        let schema = memtable.schema;
        assert_eq!(get_types(&schema), vec![InstanceType::Object]);
        let props = schema.properties.unwrap();
        assert_eq!(
            get_types(props.get("a").unwrap()),
            vec![InstanceType::Integer]
        );
        assert_eq!(
            get_types(props.get("b").unwrap()),
            vec![InstanceType::String]
        );
    }
}
